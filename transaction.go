package rmdb

import (
	"errors"
	"strings"
	"sync"
)

type Transaction struct { // 事务中不允许create drop use table database的操作
	db       *Database
	subTxs   map[string]*SubTx
	isUpdate bool
}

type SubTx struct {
	table     *Table
	memTables map[uint64]*Memtable
}

func (d *Database) Begin() *Transaction {
	subTxs := make(map[string]*SubTx, 16)
	for name, table := range d.tables {
		subTxs[name] = &SubTx{
			table:     table,
			memTables: make(map[uint64]*Memtable, 16),
		}
		subTxs[name].memTables[0] = &Memtable{
			lines:    make(map[uint64]Line, 16),
			isSwap:   false,
			isOrigin: false,
		}
	}
	return &Transaction{
		db:       d,
		subTxs:   subTxs,
		isUpdate: false,
	}
}

func (t *Transaction) Commit() error {
	if t.isUpdate {
		for tableName, subTx := range t.subTxs {
			table := t.db.tables[tableName]

			table.cache.lock.Lock()

			for pageId, memTable := range subTx.memTables {
				if pageId != 0 && !memTable.isOrigin {
					page, err := table.cache.GetPage(pageId)
					if err != nil {
						return err
					}
					tmp := page.lines // 交换page和memtable的lines
					page.lines = memTable.lines
					memTable.lines = tmp
					page.isDirty = true
					memTable.isSwap = true
				}
			}
			// 先update和delete再insert
			for _, line := range subTx.memTables[0].lines {
				if table.cache.pageId == 1 { //cache.pageid是下一个page的id
					page, err := table.cache.NewPage()
					if err != nil {
						return err
					}
					page.InsertLine(line)
				} else {
					page, err := table.cache.GetPage(table.cache.pageId - 1)
					if err != nil {
						return err
					}
					inserted := page.InsertLine(line)
					if !inserted {
						page, err = table.cache.NewPage()
						if err != nil {
							return err
						}
						page.InsertLine(line)
					}
				}
			}

			table.cache.lock.Unlock()

		}
	}
	return nil
}

func (t *Transaction) Rollback() error {
	for tableName, subTx := range t.subTxs {
		table := t.db.tables[tableName]

		table.cache.lock.Lock()

		for i := uint64(1); i < uint64(len(subTx.memTables)); i++ {
			if memTable, ok := subTx.memTables[i]; ok && memTable.isSwap {
				page, err := table.cache.GetPage(i)
				if err != nil {
					return err
				}
				tmp := page.lines
				page.lines = memTable.lines
				memTable.lines = tmp
			}
		}

		for _, line := range subTx.memTables[0].lines {
			page, err := table.cache.GetPage(line.pageId)
			if err != nil {
				return err
			}
			delete(page.lines, line.lineId)
			page.isDirty = true
		}

		table.cache.lock.Unlock()

	}
	return nil
}

func (t *Transaction) Update(sql string) error {
	t.isUpdate = true
	sqlBytes := []byte(sql + "\n")

	t.db.walLock.Lock()
	_, err := t.db.wal.Write(sqlBytes)
	t.db.walLock.Unlock()

	if err != nil {
		return err
	}
	sql = strings.Trim(sql, "; ")
	success := CheckParentheses(sql)
	if !success {
		return errors.New("invaild parentheses")
	}
	sqls := strings.Split(sql, ";")
	for _, uni := range sqls {
		uni = strings.Trim(uni, "\n\t ")
		if len(uni) == 0 {
			continue
		}
		err = t.CompileUpdate(uni)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Transaction) Query(sql string) (*ResultSet, error) {
	var wg sync.WaitGroup
	sql = strings.Trim(sql, "; ")
	success := CheckParentheses(sql)
	if !success {
		return nil, errors.New("invaild parentheses")
	}
	slice := ConvertQuery(sql)
	logicalPlans, outOuder, tableName, err := t.CompileQuery(slice, &wg)
	if err != nil {
		return nil, err
	}
	resultSet := execute(logicalPlans, outOuder, &wg)

	table := t.db.tables[tableName]
	subTx := t.subTxs[tableName]
	table.cache.lock.Lock()
	for _, line := range resultSet.result { // 根据局部性原理，筛选出的line大多都在同一页，所以不会复制太多页
		if _, ok := subTx.memTables[line.pageId]; !ok {
			memTable, err := table.cache.CopyPage(line.pageId)
			if err != nil {
				return nil, err
			}
			subTx.memTables[line.pageId] = memTable
		}
	}
	table.cache.lock.Unlock()

	return resultSet, nil
}
