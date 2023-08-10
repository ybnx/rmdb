package rmdb

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

type Database struct {
	dbName     string
	dbPath     string
	tables     map[string]*Table
	CondiFuncs map[string]func([]any) bool
	ColFuncs   map[string]func(any) any
	AggFuncs   map[string]func([]any) any
	ExecFuncs  map[string]func([]any) any
	wal        FileIO
	walLock    sync.RWMutex
}

const (
	_ = 1 << (10 * iota)
	KIB
	MIB
	GIB
)

func CreateDatabase(name string) (*Database, error) {
	GlobalOption.lock.Lock()
	defer GlobalOption.lock.Unlock()
	dbPath := fmt.Sprint(GlobalOption.Root, string(os.PathSeparator), name)
	if _, err := os.Stat(dbPath); err != nil && os.IsNotExist(err) {
		err := os.MkdirAll(dbPath, 0644)
		if err != nil {
			return nil, err
		}
		walPath := fmt.Sprint(dbPath, string(os.PathSeparator), "wal.txt")
		walFile, err := OpenFile(walPath)
		if err != nil {
			return nil, err
		}
		db := &Database{
			dbName:     name,
			dbPath:     dbPath,
			tables:     make(map[string]*Table, 64),
			CondiFuncs: make(map[string]func([]any) bool, 64),
			ColFuncs:   make(map[string]func(any) any, 64),
			AggFuncs:   make(map[string]func([]any) any, 64),
			ExecFuncs:  make(map[string]func([]any) any, 64),
			wal:        walFile,
		}
		db.setFunctions()
		databases[name] = db
		return db, nil
	} else {
		return nil, errors.New("database is exists")
	}
}

func UseDatabase(name string) (*Database, error) {
	GlobalOption.lock.Lock()
	defer GlobalOption.lock.Unlock()
	oldDB := databases[name]
	if oldDB != nil {
		return oldDB, nil
	}
	dbPath := fmt.Sprint(GlobalOption.Root, string(os.PathSeparator), name)
	if _, err := os.Stat(dbPath); err != nil {
		return nil, errors.New("database not exists")
	}
	logPath := fmt.Sprint(dbPath, string(os.PathSeparator), "cata.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, err
	}
	var tables map[string]Table
	err = json.Unmarshal(data, &tables)
	if err != nil {
		return nil, err
	}
	tabPts := make(map[string]*Table, 64)
	for tabName, table := range tables { // TODO !!!注意
		table := table
		tabPts[tabName] = &table
	}
	for tabName, table := range tabPts {
		tabPath := fmt.Sprint(dbPath, string(os.PathSeparator), tabName, ".data")
		tabFile, err := OpenFile(tabPath)
		if err != nil {
			return nil, err
		}
		table.file = tabFile
		cache := &LruCache{
			pageList: list.New(),
			pageMap:  make(map[uint64]*list.Element, 16),
			pageId:   uint64(len(table.Catalog) + 1), //这里加一是因为pageId代表下一个page的id，而pageId从1开始
			pageNum:  0,
			maxPage:  GlobalOption.MaxPage,
			maxLine:  GlobalOption.MaxLine,
		}
		table.cache = cache
		cache.table = table
	}
	walPath := fmt.Sprint(dbPath, string(os.PathSeparator), "wal.txt")
	walFile, err := OpenFile(walPath)
	if err != nil {
		return nil, err
	}
	db := &Database{
		dbName:     name,
		dbPath:     dbPath,
		tables:     tabPts,
		CondiFuncs: make(map[string]func([]any) bool, 64),
		ColFuncs:   make(map[string]func(any) any, 64),
		AggFuncs:   make(map[string]func([]any) any, 64),
		ExecFuncs:  make(map[string]func([]any) any, 64),
		wal:        walFile,
	}
	db.setFunctions()
	databases[name] = db
	return db, nil
}

func DropDatabase(name string) error {
	GlobalOption.lock.Lock()
	defer GlobalOption.lock.Unlock()
	dbPath := fmt.Sprint(GlobalOption.Root, string(os.PathSeparator), name)
	if _, err := os.Stat(dbPath); err != nil && os.IsNotExist(err) {
		return errors.New("database not exists")
	} else {
		delete(databases, name)
		return os.RemoveAll(dbPath)
	}
}

func (d *Database) Close() error {
	GlobalOption.lock.Lock()
	defer GlobalOption.lock.Unlock()
	for _, table := range d.tables {
		var err error
		err = table.Close()
		if err != nil {
			return err
		}
		if table.updated {
			err = table.Merge(d.dbPath)
			if err != nil {
				return err
			}
		} else {
			err = table.file.Close()
			if err != nil {
				return err
			}
		}
	}

	err := d.wal.Close()
	if err != nil {
		return err
	}
	err = os.Remove(d.wal.Name())
	if err != nil {
		return err
	}

	logPath := fmt.Sprint(d.dbPath, string(os.PathSeparator), "cata.log")
	if _, err = os.Stat(logPath); err == nil {
		err = os.Remove(logPath)
		if err != nil {
			return err
		}
	}
	tables := make(map[string]Table, 16)
	for tabName, table := range d.tables { // !!!
		tables[tabName] = *table
	}
	date, err := json.Marshal(tables)
	if err != nil {
		return err
	}
	logFile, err := OpenFile(logPath)
	if err != nil {
		return err
	}
	_, err = logFile.Write(date)
	if err != nil {
		return err
	}
	err = logFile.Close()
	if err != nil {
		return err
	}
	delete(databases, d.dbName)
	return nil
}

func (d *Database) setFunctions() { // 深拷贝 不需要加锁，create和use加锁了
	for name, method := range GlobalOption.CondiFuncs {
		d.CondiFuncs[name] = method
	}
	for name, method := range GlobalOption.ColFuncs {
		d.ColFuncs[name] = method
	}
	for name, method := range GlobalOption.AggFuncs {
		d.AggFuncs[name] = method
	}
	for name, method := range GlobalOption.ExecFuncs {
		d.ExecFuncs[name] = method
	}
}
