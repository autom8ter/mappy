# mappy
--
    import "github.com/autom8ter/mappy"


## Usage

```go
var DefaultOpts = &Opts{
	Path:    "/tmp/mappy",
	Restore: true,
}
```

```go
var Done = errors.New("mappy: done")
```

#### type Bucket

```go
type Bucket interface {
	Path() []string
	Record(opts *RecordOpts) *Record
	Nest(opts *NestOpts) Bucket
	Del(opts *DelOpts) error
	Flush(opts *FlushOpts) error
	Len(opts *LenOpts) int
	Get(opts *GetOpts) (value *Record, ok bool)
	Set(opts *SetOpts) error
	View(opts *ViewOpts) error
}
```


#### type ChangeHandlerFunc

```go
type ChangeHandlerFunc func(bucket Bucket, log *Log) error
```


#### type CloseOpts

```go
type CloseOpts struct {
}
```


#### type DelOpts

```go
type DelOpts struct {
	Key interface{}
}
```


#### type DestroyOpts

```go
type DestroyOpts struct {
}
```


#### type FlushOpts

```go
type FlushOpts struct {
}
```


#### type GetOpts

```go
type GetOpts struct {
	Key string
}
```


#### type LenOpts

```go
type LenOpts struct {
}
```


#### type Log

```go
type Log struct {
	Sequence  int
	Op        Op
	Record    *Record
	CreatedAt time.Time
}
```


#### type Mappy

```go
type Mappy interface {
	Bucket
	Bucket(path []string) Bucket
	Close(opts *CloseOpts) error

	DestroyLogs(opts *DestroyOpts) error
	ReplayLogs(opts *ReplayOpts) error
	BackupLogs(w io.Writer) (int64, error)
}
```


#### func  Open

```go
func Open(opts *Opts) (Mappy, error)
```

#### type NestOpts

```go
type NestOpts struct {
	Key string
}
```


#### type Op

```go
type Op int
```


```go
const (
	DELETE Op = 2
	SET    Op = 3
)
```

#### type Opts

```go
type Opts struct {
	Path    string
	Restore bool
}
```


#### type Record

```go
type Record struct {
	Key        interface{} `json:"key"`
	Val        interface{} `json:"val"`
	BucketPath []string    `json:"bucketPath"`
	GloablId   string      `json:"globalId"`
	UpdatedAt  time.Time   `json:"updatedAt"`
}
```


#### type RecordOpts

```go
type RecordOpts struct {
	Key interface{}
	Val interface{}
}
```


#### type ReplayFunc

```go
type ReplayFunc func(bucket Bucket, lg *Log) error
```


#### type ReplayOpts

```go
type ReplayOpts struct {
	Min int
	Max int
	Fn  ReplayFunc
}
```


#### type RestoreOpts

```go
type RestoreOpts struct {
}
```


#### type SetOpts

```go
type SetOpts struct {
	Record *Record
}
```


#### type ViewFunc

```go
type ViewFunc func(bucket Bucket, record *Record) error
```


#### type ViewOpts

```go
type ViewOpts struct {
	Fn ViewFunc
}
```
