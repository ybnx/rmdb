package rmdb

import (
	"container/list"
	"errors"
	"fmt"
	"os"
)

type Table struct {
	Name    string
	file    FileIO
	Columns []Column
	cache   *LruCache
	Catalog map[uint64]Page
	txs     map[uint64]*Transaction
	txId    uint64
	updated bool
}

func (d *Database) CreateTable(name string) (*Table, error) {
	GlobalOption.lock.Lock()
	defer GlobalOption.lock.Unlock()
	if _, ok := d.tables[name]; ok {
		return nil, errors.New("table is exists")
	}
	tabPath := fmt.Sprint(d.dbPath, string(os.PathSeparator), name, ".data")
	tabFile, err := OpenFile(tabPath)
	if err != nil {
		return nil, err
	}
	cache := &LruCache{
		pageList: list.New(),
		pageMap:  make(map[uint64]*list.Element, 16),
		pageId:   1, //从1开始
		pageNum:  0,
		maxPage:  GlobalOption.MaxPage,
		maxLine:  GlobalOption.MaxLine,
	}
	table := &Table{
		Name:    name,
		file:    tabFile,
		Columns: make([]Column, 0, 64),
		cache:   cache,
		Catalog: make(map[uint64]Page, 64),
	}
	cache.table = table
	d.tables[name] = table
	return table, nil
}

func (t *Table) SetColumn(name string, typeOf int) error {
	GlobalOption.lock.Lock()
	defer GlobalOption.lock.Unlock()
	if typeOf < BOOL || typeOf > DATE {
		return errors.New("unsupported type")
	}
	defVal, err := defaultValue(typeOf)
	if err != nil {
		return err
	}
	column := Column{
		Name:   name,
		TypeOf: typeOf,
		DefVal: defVal,
	}
	t.Columns = append(t.Columns, column)
	return nil
}

func (d *Database) DropTable(name string) error {
	GlobalOption.lock.Lock()
	defer GlobalOption.lock.Unlock()
	if table, ok := d.tables[name]; ok {
		delete(d.tables, name)
		err := table.file.Close()
		if err != nil {
			return err
		}
		return os.Remove(table.file.Name())
	} else {
		return errors.New("table not exists")
	}
}

func (t *Table) Close() error {
	for len(t.cache.pageMap) != 0 {
		err := t.cache.RemoveOld()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Merge(dbPath string) error {
	tabPath := fmt.Sprint(dbPath, string(os.PathSeparator), t.Name, ".tmp")
	tabFile, err := OpenFile(tabPath)
	if err != nil {
		return err
	}
	offset := uint64(0)
	for id, page := range t.Catalog {
		if page.Length > 0 {

			data := make([]byte, page.Length)
			_, err = t.file.ReadAt(data, int64(page.Offset))
			if err != nil {
				return err
			}
			_, err = tabFile.Write(data)
			if err != nil {
				return err
			}
			t.Catalog[id] = Page{
				Id:     page.Id,
				Offset: offset,
				Length: page.Length,
			}
			offset += page.Length

		} else {
			delete(t.Catalog, id)
		}
	}
	err = tabFile.Close()
	if err != nil {
		return err
	}
	err = t.file.Close()
	if err != nil {
		return err
	}
	oldPath := t.file.Name()
	err = os.Remove(oldPath)
	if err != nil {
		return err
	}
	err = os.Rename(tabPath, oldPath)
	if err != nil {
		return err
	}
	return nil
}
