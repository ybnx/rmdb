package rmdb

import (
	"os"
	"sync"
)

type Option struct {
	Root             string
	IOMode           int
	CondiFuncs       map[string]func([]any) bool
	ColFuncs         map[string]func(any) any
	AggFuncs         map[string]func([]any) any
	ExecFuncs        map[string]func([]any) any
	MmapSize         int64
	MaxPage, MaxLine uint64
	lock             sync.RWMutex // 全局锁，意味着要想并发安全，全局必须之使用一个数据库变量
}

const (
	Standard = iota
	MMapMode
)

var (
	GlobalOption = &Option{
		Root:       "E:\\golangProject\\demo2\\dbtest",
		IOMode:     Standard,
		CondiFuncs: make(map[string]func([]any) bool, 64),
		ColFuncs:   make(map[string]func(any) any, 64),
		AggFuncs:   make(map[string]func([]any) any, 64),
		ExecFuncs:  make(map[string]func([]any) any, 64),
		MmapSize:   16 * MIB,
		MaxPage:    4,
		MaxLine:    4,
	}
	databases = make(map[string]*Database, 128)
	logger    = NewLogger(os.Stderr, "")
)

func (o *Option) SetCondiFunc(name string, function func([]any) bool) {
	o.CondiFuncs[name] = function
}

func (o *Option) SetColFunc(name string, function func(any) any) {
	o.ColFuncs[name] = function
}

func (o *Option) SetAggFunc(name string, function func([]any) any) {
	o.AggFuncs[name] = function
}

func (o *Option) SetExecFunc(name string, function func([]any) any) {
	o.ExecFuncs[name] = function
}
