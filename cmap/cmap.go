package cmap

import (
	"encoding/gob"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"
)

func init() {
	gob.Register(map[string]interface{}{})
}

type Entries map[string]*Entry

// Map is like a Go map[interface{}]interface{} but is safe for concurrent use
// by multiple goroutines without additional locking or coordination.
// Loads, stores, and deletes run in amortized constant time.
//
// The Map type is specialized. Most code should use a plain Go map instead,
// with separate locking or coordination, for better type safety and to make it
// easier to maintain other invariants along with the map content.
//
// The Map type is optimized for two common use cases: (1) when the Entry for a given
// key is only ever written once but read many times, as in caches that only grow,
// or (2) when multiple goroutines read, write, and overwrite entries for disjoint
// sets of keys. In these two cases, use of a Map may significantly reduce lock
// contention compared to a Go map paired with a separate Mutex or RWMutex.
//
// The zero Map is empty and ready for use. A Map must not be copied after first use.
type Map struct {
	mu sync.Mutex

	// read contains the portion of the map's contents that are safe for
	// concurrent access (with or without mu held).
	//
	// The read field itself is always safe to load, but must only be stored with
	// mu held.
	//
	// Entries stored in read may be updated concurrently without mu, but updating
	// a previously-expunged Entry requires that the Entry be copied to the dirty
	// map and unexpunged with mu held.
	read atomic.Value // readOnly

	// dirty contains the portion of the map's contents that require mu to be
	// held. To ensure that the dirty map can be promoted to the read map quickly,
	// it also includes all of the non-expunged entries in the read map.
	//
	// Expunged entries are not stored in the dirty map. An expunged Entry in the
	// clean map must be unexpunged and added to the dirty map before a new value
	// can be stored to it.
	//
	// If the dirty map is nil, the next write to the map will initialize it by
	// making a shallow copy of the clean map, omitting stale entries.
	dirty Entries

	// misses counts the number of loads since the read map was last updated that
	// needed to lock mu to determine whether the key was present.
	//
	// Once enough misses have occurred to cover the cost of copying the dirty
	// map, the dirty map will be promoted to the read map (in the unamended
	// state) and the next store to the map will make a new dirty copy.
	misses int
}

// readOnly is an immutable struct stored atomically in the Map.read field.
type readOnly struct {
	M       Entries
	Amended bool // true if the dirty map contains some key not in m.
}

// expunged is an arbitrary pointer that marks entries which have been deleted
// from the dirty map.
var expunged = unsafe.Pointer(new(interface{}))

// An Entry is a slot in the map corresponding to a particular key.
type Entry struct {
	// p points to the interface{} value stored for the Entry.
	//
	// If p == nil, the Entry has been deleted and m.dirty == nil.
	//
	// If p == expunged, the Entry has been deleted, m.dirty != nil, and the Entry
	// is missing from m.dirty.
	//
	// Otherwise, the Entry is valid and recorded in m.read.m[key] and, if m.dirty
	// != nil, in m.dirty[key].
	//
	// An Entry can be deleted by atomic replacement with nil: when m.dirty is
	// next created, it will atomically replace nil with expunged and leave
	// m.dirty[key] unset.
	//
	// An Entry's associated value can be updated by atomic replacement, provided
	// p != expunged. If p == expunged, an Entry's associated value can be updated
	// only after first setting m.dirty[key] = e so that lookups using the dirty
	// map find the Entry.
	P unsafe.Pointer // *interface{}
}

func newEntry(i interface{}) *Entry {
	return &Entry{P: unsafe.Pointer(&i)}
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *Map) Load(key string) (value interface{}, ok bool) {
	read, _ := m.read.Load().(readOnly)
	e, ok := read.M[key]
	if !ok && read.Amended {
		m.mu.Lock()
		// Avoid reporting a spurious miss if m.dirty got promoted while we were
		// blocked on m.mu. (If further loads of the same key will not miss, it's
		// not worth copying the dirty map for this key.)
		read, _ = m.read.Load().(readOnly)
		e, ok = read.M[key]
		if !ok && read.Amended {
			e, ok = m.dirty[key]
			// Regardless of whether the Entry was present, record a miss: this key
			// will take the slow path until the dirty map is promoted to the read
			// map.
			m.missLocked()
		}
		m.mu.Unlock()
	}
	if !ok {
		return nil, false
	}
	return e.load()
}

func (e *Entry) load() (value interface{}, ok bool) {
	p := atomic.LoadPointer(&e.P)
	if p == nil || p == expunged {
		return nil, false
	}
	return *(*interface{})(p), true
}

// Store sets the value for a key.
func (m *Map) Store(key string, value interface{}) {
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.M[key]; ok && e.tryStore(&value) {
		return
	}

	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)
	if e, ok := read.M[key]; ok {
		if e.unexpungeLocked() {
			// The Entry was previously expunged, which implies that there is a
			// non-nil dirty map and this Entry is not in it.
			m.dirty[key] = e
		}
		e.storeLocked(&value)
	} else if e, ok := m.dirty[key]; ok {
		e.storeLocked(&value)
	} else {
		if !read.Amended {
			// We're adding the first new key to the dirty map.
			// Make sure it is allocated and mark the read-only map as incomplete.
			m.dirtyLocked()
			m.read.Store(readOnly{M: read.M, Amended: true})
		}
		m.dirty[key] = newEntry(value)
	}
	m.mu.Unlock()
}

// tryStore stores a value if the Entry has not been expunged.
//
// If the Entry is expunged, tryStore returns false and leaves the Entry
// unchanged.
func (e *Entry) tryStore(i *interface{}) bool {
	for {
		p := atomic.LoadPointer(&e.P)
		if p == expunged {
			return false
		}
		if atomic.CompareAndSwapPointer(&e.P, p, unsafe.Pointer(i)) {
			return true
		}
	}
}

// unexpungeLocked ensures that the Entry is not marked as expunged.
//
// If the Entry was previously expunged, it must be added to the dirty map
// before m.mu is unlocked.
func (e *Entry) unexpungeLocked() (wasExpunged bool) {
	return atomic.CompareAndSwapPointer(&e.P, expunged, nil)
}

// storeLocked unconditionally stores a value to the Entry.
//
// The Entry must be known not to be expunged.
func (e *Entry) storeLocked(i *interface{}) {
	atomic.StorePointer(&e.P, unsafe.Pointer(i))
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *Map) LoadOrStore(key string, value interface{}) (actual interface{}, loaded bool) {
	// Avoid locking if it's a clean hit.
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.M[key]; ok {
		actual, loaded, ok := e.tryLoadOrStore(value)
		if ok {
			return actual, loaded
		}
	}

	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)
	if e, ok := read.M[key]; ok {
		if e.unexpungeLocked() {
			m.dirty[key] = e
		}
		actual, loaded, _ = e.tryLoadOrStore(value)
	} else if e, ok := m.dirty[key]; ok {
		actual, loaded, _ = e.tryLoadOrStore(value)
		m.missLocked()
	} else {
		if !read.Amended {
			// We're adding the first new key to the dirty map.
			// Make sure it is allocated and mark the read-only map as incomplete.
			m.dirtyLocked()
			m.read.Store(readOnly{M: read.M, Amended: true})
		}
		m.dirty[key] = newEntry(value)
		actual, loaded = value, false
	}
	m.mu.Unlock()

	return actual, loaded
}

// tryLoadOrStore atomically loads or stores a value if the Entry is not
// expunged.
//
// If the Entry is expunged, tryLoadOrStore leaves the Entry unchanged and
// returns with ok==false.
func (e *Entry) tryLoadOrStore(i interface{}) (actual interface{}, loaded, ok bool) {
	p := atomic.LoadPointer(&e.P)
	if p == expunged {
		return nil, false, false
	}
	if p != nil {
		return *(*interface{})(p), true, true
	}

	// Copy the interface after the first load to make this method more amenable
	// to escape analysis: if we hit the "load" path or the Entry is expunged, we
	// shouldn't bother heap-allocating.
	ic := i
	for {
		if atomic.CompareAndSwapPointer(&e.P, nil, unsafe.Pointer(&ic)) {
			return i, false, true
		}
		p = atomic.LoadPointer(&e.P)
		if p == expunged {
			return nil, false, false
		}
		if p != nil {
			return *(*interface{})(p), true, true
		}
	}
}

// Delete deletes the value for a key.
func (m *Map) Delete(key string) {
	read, _ := m.read.Load().(readOnly)
	e, ok := read.M[key]
	if !ok && read.Amended {
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		e, ok = read.M[key]
		if !ok && read.Amended {
			delete(m.dirty, key)
		}
		m.mu.Unlock()
	}
	if ok {
		e.delete()
	}
}

func (e *Entry) delete() (hadValue bool) {
	for {
		p := atomic.LoadPointer(&e.P)
		if p == nil || p == expunged {
			return false
		}
		if atomic.CompareAndSwapPointer(&e.P, p, nil) {
			return true
		}
	}
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the Map's
// contents: no key will be visited more than once, but if the value for any key
// is stored or deleted concurrently, Range may reflect any mapping for that key
// from any point during the Range call.
//
// Range may be O(N) with the number of elements in the map even if f returns
// false after a constant number of calls.
func (m *Map) Range(f func(key string, value interface{}) bool) {
	// We need to be able to iterate over all of the keys that were already
	// present at the start of the call to Range.
	// If read.amended is false, then read.m satisfies that property without
	// requiring us to hold m.mu for a long time.
	read, _ := m.read.Load().(readOnly)
	if read.Amended {
		// m.dirty contains keys not in read.m. Fortunately, Range is already O(N)
		// (assuming the caller does not break out early), so a call to Range
		// amortizes an entire copy of the map: we can promote the dirty copy
		// immediately!
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		if read.Amended {
			read = readOnly{M: m.dirty}
			m.read.Store(read)
			m.dirty = nil
			m.misses = 0
		}
		m.mu.Unlock()
	}

	for k, e := range read.M {
		v, ok := e.load()
		if !ok {
			continue
		}
		if !f(k, v) {
			break
		}
	}
}

func (m *Map) missLocked() {
	m.misses++
	if m.misses < len(m.dirty) {
		return
	}
	m.read.Store(readOnly{M: m.dirty})
	m.dirty = nil
	m.misses = 0
}

func (m *Map) dirtyLocked() {
	if m.dirty != nil {
		return
	}

	read, _ := m.read.Load().(readOnly)
	m.dirty = make(map[string]*Entry, len(read.M))
	for k, e := range read.M {
		if !e.tryExpungeLocked() {
			m.dirty[k] = e
		}
	}
}

func (e *Entry) tryExpungeLocked() (isExpunged bool) {
	p := atomic.LoadPointer(&e.P)
	for p == nil {
		if atomic.CompareAndSwapPointer(&e.P, nil, expunged) {
			return true
		}
		p = atomic.LoadPointer(&e.P)
	}
	return p == expunged
}

func (m *Map) Encode(w io.Writer) error {
	mmap := map[string]interface{}{}
	m.Range(func(key string, value interface{}) bool {
		mmap[key] = value
		return true
	})
	return gob.NewEncoder(w).Encode(mmap)
}

func (m *Map) Decode(r io.Reader) error {
	mmap := map[string]interface{}{}
	if err := gob.NewDecoder(r).Decode(&mmap); err != nil {
		return err
	}
	for k, v := range mmap {
		m.Store(k, v)
	}
	return nil
}
