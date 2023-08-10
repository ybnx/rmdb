package rmdb

import (
	"fmt"
	"github.com/liushuochen/gotable"
	"os"
	"sync"
)

type ResultSet struct {
	result   []*Line
	outOuder []string
}

func execute(logicalPlans map[int]Plan, outOuder []string, wg *sync.WaitGroup) *ResultSet {
	physicalPlans := make([]Plan, 0, 16)
	for symbol := 0; symbol < 11; symbol++ {
		if plan, ok := logicalPlans[symbol]; ok && plan.getConfig() {
			physicalPlans = append(physicalPlans, plan)
		}
	}
	for i := 1; i < len(physicalPlans); i++ {
		physicalPlans[i].setChild(physicalPlans[i-1].getParent())
	}
	output := physicalPlans[len(physicalPlans)-1].getParent()
	resultSet := &ResultSet{
		result:   make([]*Line, 0, 64),
		outOuder: outOuder,
	}
	wg.Add(len(physicalPlans) + 1)
	for _, plan := range physicalPlans {
		go plan.process()
	}
	go func() {
		defer wg.Done()
		for line := range output {
			resultSet.result = append(resultSet.result, line)
		}
	}()
	wg.Wait()
	return resultSet
}

func (d *Database) Update(sql string) error {
	tx := d.Begin()
	err := tx.Update(sql)
	if err != nil { // TODO 这里没有commit应该不用回滚
		return err
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

func (d *Database) Query(sql string) (*ResultSet, error) {
	tx := d.Begin()
	return tx.Query(sql) // query事务可以不commit，因为进入commit方法后直接返回
}

func ShowDatabase() string {
	dirEns, err := os.ReadDir(GlobalOption.Root)
	if err != nil {
		logger.Error(err)
		return ""
	}
	table, err := gotable.Create("Database")
	if err != nil {
		logger.Error(err)
		return ""
	}
	for _, dirEn := range dirEns {
		row := map[string]string{"Database": dirEn.Name()}
		err = table.AddRow(row)
		if err != nil {
			logger.Error(err)
			return ""
		}
	}
	return fmt.Sprint(table)
}

func (d *Database) ShowTables() string {
	table, err := gotable.Create(d.dbName)
	if err != nil {
		logger.Error(err)
		return ""
	}
	for name, _ := range d.tables {
		row := map[string]string{d.dbName: name}
		err = table.AddRow(row)
		if err != nil {
			logger.Error(err)
			return ""
		}
	}
	return fmt.Sprint(table)
}

func (r *ResultSet) ToString() string {
	table, err := gotable.Create(r.outOuder...)
	if err != nil {
		logger.Error(err)
	}
	for _, line := range r.result {
		row := make(map[string]string, 64)
		for _, colName := range r.outOuder {
			row[colName] = string(line.nameToVal[colName].value)
		}
		err = table.AddRow(row)
		if err != nil {
			logger.Error(err)
		}
	}
	return fmt.Sprint(table)
}
