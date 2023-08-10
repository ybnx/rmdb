package rmdb

import (
	"crypto/sha256"
	"sort"
	"strings"
	"sync"
)

type Plan interface {
	process()
	setChild(childline chan *Line)
	getParent() chan *Line
	getConfig() bool //判断是否配置
}

type basePlan struct {
	wg                      *sync.WaitGroup
	childlines, parentlines chan *Line
	isConfig                bool
}

const (
	TableRead = iota
	Projection
	Selection
	Aggregation
	FuncCol
	Execute
	Rename
	Having
	Distinct
	Sorting
	Limit
)

type TableReadPlan struct {
	basePlan
	tx        *Transaction
	tableName string
}

func (t *TableReadPlan) process() {
	defer func() {
		close(t.parentlines)
		t.wg.Done()
	}()
	if _, ok := t.tx.db.tables[t.tableName]; !ok {
		return
	}
	table := t.tx.db.tables[t.tableName]
	if table == nil {
		logger.Error("invalid table name")
	}
	subTx := t.tx.subTxs[t.tableName]

	table.cache.lock.Lock()

	for i := uint64(1); i < table.cache.pageId; i++ {

		memTable := subTx.memTables[i]
		var lines map[uint64]Line
		if memTable != nil {
			lines = memTable.lines
		} else {
			page, err := table.cache.GetPage(i)
			if err != nil {
				logger.Error(err)
				break
			}
			if page == nil {
				continue
			}
			lines = page.lines
		}

		for _, line := range lines {
			line := line
			t.parentlines <- &line
		}

	}

	table.cache.lock.Unlock()

	for _, line := range subTx.memTables[0].lines {
		line := line
		t.parentlines <- &line
	}

}

func (t *TableReadPlan) setChild(childline chan *Line) {
	// no child
}

func (t *TableReadPlan) getParent() chan *Line {
	return t.parentlines
}

func (t *TableReadPlan) getConfig() bool {
	return t.isConfig
}

type SelectionPlan struct {
	basePlan
	colToFuncs []struct {
		colNames []string
		funcName string
	}
	selFuncs map[string]func([]any) bool
}

func (s *SelectionPlan) process() {
	defer func() {
		close(s.parentlines)
		s.wg.Done()
	}()
	for line := range s.childlines {
		pass := true
		oldVals := make([]any, 0, 64)
		for _, colToFunc := range s.colToFuncs {
			for _, colName := range colToFunc.colNames {
				oldVal, err := DecodeData(line.nameToVal[colName].value, line.nameToVal[colName].column.TypeOf)
				if err != nil {
					logger.Error(err)
				}
				oldVals = append(oldVals, oldVal)
			}
			if !s.selFuncs[colToFunc.funcName](oldVals) {
				pass = false
				break
			}
		}
		if pass {
			s.parentlines <- line
		}
	}
}

func (s *SelectionPlan) setChild(childline chan *Line) {
	s.childlines = childline
}

func (s *SelectionPlan) getParent() chan *Line {
	return s.parentlines
}

func (s *SelectionPlan) getConfig() bool {
	return s.isConfig
}

type ProjectionPlan struct {
	basePlan
	colNames map[string]struct{} // 用set不用arr是为了避免列名重复
}

func (p *ProjectionPlan) process() {
	defer func() {
		close(p.parentlines)
		p.wg.Done()
	}()
	for line := range p.childlines {
		newLine := &Line{
			nameToVal: make(map[string]ColVal, 16),
			pageId:    line.pageId,
			lineId:    line.lineId,
		}
		for colName, colToVal := range line.nameToVal {
			if _, ok := p.colNames[colName]; ok {
				newLine.nameToVal[colName] = colToVal
			}
			if strings.Contains(colName, "(") {
				newLine.nameToVal[colName] = colToVal
			}
		}
		p.parentlines <- newLine
	}
}

func (p *ProjectionPlan) setChild(childline chan *Line) {
	p.childlines = childline
}

func (p *ProjectionPlan) getParent() chan *Line {
	return p.parentlines
}

func (p *ProjectionPlan) getConfig() bool {
	return p.isConfig
}

type RenamePlan struct {
	basePlan
	oldToNew map[string]string
}

func (r *RenamePlan) process() {
	defer func() {
		close(r.parentlines)
		r.wg.Done()
	}()
	for line := range r.childlines {
		for oldName, newName := range r.oldToNew {
			line.nameToVal[newName] = line.nameToVal[oldName]
		}
		r.parentlines <- line
	}
}

func (r *RenamePlan) setChild(childline chan *Line) {
	r.childlines = childline
}

func (r *RenamePlan) getParent() chan *Line {
	return r.parentlines
}

func (r *RenamePlan) getConfig() bool {
	return r.isConfig
}

type DistinctPlan struct {
	basePlan
}

func (d *DistinctPlan) process() {
	defer func() {
		close(d.parentlines)
		d.wg.Done()
	}()
	hashs := make(map[[32]byte]struct{}, 64)
	colNames := make([]string, 0, 64)
	for line := range d.childlines {
		if len(colNames) == 0 {
			for colName, _ := range line.nameToVal {
				colNames = append(colNames, colName)
			}
		}
		lineBytes := make([]byte, 0, 64)
		for _, colName := range colNames {
			name, err := EncodeData(line.nameToVal[colName].column.Name)
			if err != nil {
				logger.Error(err)
			}
			typeOf, err := EncodeData(line.nameToVal[colName].column.TypeOf)
			if err != nil {
				logger.Error(err)
			}
			defVal, err := EncodeData(line.nameToVal[colName].column.DefVal)
			if err != nil {
				logger.Error(err)
			}
			lineBytes = append(lineBytes, name...)
			lineBytes = append(lineBytes, typeOf...)
			lineBytes = append(lineBytes, defVal...)
			lineBytes = append(lineBytes, line.nameToVal[colName].value...)
		}
		hash := sha256.Sum256(lineBytes)
		if _, ok := hashs[hash]; !ok {
			hashs[hash] = struct{}{}
			d.parentlines <- line
		}
	}
}

func (d *DistinctPlan) setChild(childline chan *Line) {
	d.childlines = childline
}

func (d *DistinctPlan) getParent() chan *Line {
	return d.parentlines
}

func (d *DistinctPlan) getConfig() bool {
	return d.isConfig
}

type AggregationPlan struct {
	basePlan
	byCols     []string
	colToFuncs []struct {
		colName, funcName string
	}
	aggFuncs map[string]func([]any) any
}

func (a *AggregationPlan) process() {
	defer func() {
		close(a.parentlines)
		a.wg.Done()
	}()
	aggMap := make(map[[32]byte][]*Line, 64)
	for line := range a.childlines {
		valsBytes := make([]byte, 0, 64)
		for _, byCol := range a.byCols {
			valsBytes = append(valsBytes, line.nameToVal[byCol].value...)
		}
		hash := sha256.Sum256(valsBytes)
		aggMap[hash] = append(aggMap[hash], line)
	}
	for _, lines := range aggMap {
		newLine := lines[0]
		for _, colToFunc := range a.colToFuncs {
			oldVals := make([]any, 0, 64)
			for _, line := range lines {
				oldVal, err := DecodeData(line.nameToVal[colToFunc.colName].value, line.nameToVal[colToFunc.colName].column.TypeOf)
				if err != nil {
					logger.Error(err)
				}
				oldVals = append(oldVals, oldVal)
			}
			newVal := a.aggFuncs[colToFunc.funcName](oldVals)
			newData, err := EncodeData(newVal)
			if err != nil {
				logger.Error(err)
			}
			newColName := NewColName(colToFunc.funcName, colToFunc.colName)

			newLine.nameToVal[newColName] = ColVal{
				column: newLine.nameToVal[colToFunc.colName].column,
				value:  newData,
			}

		}
		a.parentlines <- newLine
	}
}

func (a *AggregationPlan) setChild(childline chan *Line) {
	a.childlines = childline
}

func (a *AggregationPlan) getParent() chan *Line {
	return a.parentlines
}

func (a *AggregationPlan) getConfig() bool {
	return a.isConfig
}

type HavingPlan struct { //必须和as结合使用
	basePlan
	colToFuncs []struct {
		colNames []string
		funcName string
	}
	havFuncs map[string]func([]any) bool
}

func (h *HavingPlan) process() {
	defer func() {
		close(h.parentlines)
		h.wg.Done()
	}()
	for line := range h.childlines {
		pass := true
		oldVals := make([]any, 0, 64)
		for _, colToFunc := range h.colToFuncs {
			for _, colName := range colToFunc.colNames {
				oldVal, err := DecodeData(line.nameToVal[colName].value, line.nameToVal[colName].column.TypeOf)
				if err != nil {
					logger.Error(err)
				}
				oldVals = append(oldVals, oldVal)
			}
			if !h.havFuncs[colToFunc.funcName](oldVals) {
				pass = false
				break
			}
		}
		if pass {
			h.parentlines <- line
		}
	}
}

func (h *HavingPlan) setChild(childline chan *Line) {
	h.childlines = childline
}

func (h *HavingPlan) getParent() chan *Line {
	return h.parentlines
}

func (h *HavingPlan) getConfig() bool {
	return h.isConfig
}

type FuncColPlan struct {
	basePlan
	colToFuncs []struct {
		colName, funcName string
	}
	colFuncs map[string]func(any) any
}

func (f *FuncColPlan) process() {
	defer func() {
		close(f.parentlines)
		f.wg.Done()
	}()
	for line := range f.childlines {
		for _, colToFunc := range f.colToFuncs {
			newColName := NewColName(colToFunc.funcName, colToFunc.colName)
			oldVal, err := DecodeData(line.nameToVal[colToFunc.colName].value, line.nameToVal[colToFunc.colName].column.TypeOf)
			if err != nil {
				logger.Error(err)
			}
			newVal := f.colFuncs[colToFunc.funcName](oldVal)
			newData, err := EncodeData(newVal)
			if err != nil {
				logger.Error(err)
			}

			line.nameToVal[newColName] = ColVal{
				column: line.nameToVal[colToFunc.colName].column,
				value:  newData,
			}

		}
		f.parentlines <- line

	}
}

func (f *FuncColPlan) setChild(childline chan *Line) {
	f.childlines = childline
}

func (f *FuncColPlan) getParent() chan *Line {
	return f.parentlines
}

func (f *FuncColPlan) getConfig() bool {
	return f.isConfig
}

type ExecutePlan struct {
	basePlan
	colToFuncs []struct {
		colNames []string
		funcName string
	}
	execFuncs map[string]func([]any) any
}

func (e *ExecutePlan) process() {
	defer func() {
		close(e.parentlines)
		e.wg.Done()
	}()
	for line := range e.childlines {
		for _, colToFunc := range e.colToFuncs {
			newColName := NewColName(colToFunc.funcName, colToFunc.colNames...)

			oldVals := make([]any, 0, 64)
			for _, colName := range colToFunc.colNames {
				oldVal, err := DecodeData(line.nameToVal[colName].value, line.nameToVal[colName].column.TypeOf)
				if err != nil {
					logger.Error(err)
				}
				oldVals = append(oldVals, oldVal)
			}
			newVal := e.execFuncs[colToFunc.funcName](oldVals)
			newData, err := EncodeData(newVal)
			if err != nil {
				logger.Error(err)
			}

			line.nameToVal[newColName] = ColVal{
				column: line.nameToVal[colToFunc.colNames[0]].column,
				value:  newData,
			}

		}
		e.parentlines <- line
	}
}

func (e *ExecutePlan) setChild(childline chan *Line) {
	e.childlines = childline
}

func (e *ExecutePlan) getParent() chan *Line {
	return e.parentlines
}

func (e *ExecutePlan) getConfig() bool {
	return e.isConfig
}

type SortingPlan struct {
	basePlan
	colNames []string
	isAsc    bool
}

func (s *SortingPlan) process() {
	defer func() {
		close(s.parentlines)
		s.wg.Done()
	}()
	sortArr := make([]string, 0, 64)
	sortMap := make(map[string][]*Line, 64)
	for line := range s.childlines {
		valsBytes := make([]byte, 0, 64)
		for _, colName := range s.colNames {
			valsBytes = append(valsBytes, line.nameToVal[colName].value...)
		}
		if _, ok := sortMap[string(valsBytes)]; !ok {
			sortArr = append(sortArr, string(valsBytes))
		}
		sortMap[string(valsBytes)] = append(sortMap[string(valsBytes)], line)
	}
	sort.Strings(sortArr)
	if s.isAsc {
		for _, val := range sortArr {
			lines := sortMap[val]
			for _, line := range lines {
				s.parentlines <- line
			}
		}
	} else {
		for i := len(sortArr) - 1; i >= 0; i-- {
			lines := sortMap[sortArr[i]]
			for _, line := range lines {
				s.parentlines <- line
			}
		}
	}
}

func (s *SortingPlan) setChild(childline chan *Line) {
	s.childlines = childline
}

func (s *SortingPlan) getParent() chan *Line {
	return s.parentlines
}

func (s *SortingPlan) getConfig() bool {
	return s.isConfig
}

type LimitPlan struct {
	basePlan
	offset, count uint64
}

func (l *LimitPlan) process() {
	defer func() {
		close(l.parentlines)
		l.wg.Done()
	}()
	for line := range l.childlines {
		if l.offset > 0 {
			l.offset -= 1
			continue
		}
		if l.count == 0 {
			break
		}
		l.parentlines <- line
		l.count -= 1
	}
}

func (l *LimitPlan) setChild(childline chan *Line) {
	l.childlines = childline
}

func (l *LimitPlan) getParent() chan *Line {
	return l.parentlines
}

func (l *LimitPlan) getConfig() bool {
	return l.isConfig
}
