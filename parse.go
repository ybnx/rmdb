package rmdb

import (
	"bytes"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ConvertQuery(sql string) []string {

	tokens := []string{"select", "distinct", "from", "where", "group", "by", "having", "order", "by", "limit"}

	pattern := "(?i)\\b(" + strings.Join(tokens, "|") + ")\\b"
	reg := regexp.MustCompile(pattern)
	newsql := reg.ReplaceAllStringFunc(sql, func(match string) string {
		return strings.ToLower(match)
	})
	indexs := make([]int, 0, 64)
	for _, token := range tokens {
		reg = regexp.MustCompile(token)
		matches := reg.FindAllStringIndex(newsql, -1)
		for _, match := range matches {
			indexs = append(indexs, match[0])
			indexs = append(indexs, match[0]+len(token))
		}
	}
	indexs = append(indexs, len(newsql))
	sort.Ints(indexs)
	slice := make([]string, 0, 16)
	for i := 0; i < len(indexs)-1; i++ {
		part := strings.TrimSpace(newsql[indexs[i]:indexs[i+1]])
		if part != "" {
			slice = append(slice, part)
		}
	}
	return slice
}

func (t *Transaction) CompileQuery(slice []string, wg *sync.WaitGroup) (map[int]Plan, []string, string, error) {
	pjp := &ProjectionPlan{
		basePlan: basePlan{
			wg:          wg,
			parentlines: make(chan *Line, 64),
			isConfig:    true,
		},
		colNames: make(map[string]struct{}, 64),
	}
	agp := &AggregationPlan{
		basePlan: basePlan{
			wg:          wg,
			parentlines: make(chan *Line, 64),
		},
		aggFuncs: make(map[string]func([]any) any, 64),
	}

	plans := make(map[int]Plan, 16)
	outOuder := make([]string, 0, 16)
	var tableName string

	for i := 0; i < len(slice); i += 2 {
		switch slice[i] {
		case "select":
			if slice[i+1] == "distinct" {
				dtp := &DistinctPlan{
					basePlan: basePlan{
						wg:          wg,
						parentlines: make(chan *Line, 64),
						isConfig:    true,
					},
				}
				plans[Distinct] = dtp
				i += 1
			}
			reg := regexp.MustCompile(`(?:[^,(]|\([^)]*\))+`)
			parts := reg.FindAllString(slice[i+1], -1)
			rnp := &RenamePlan{
				basePlan: basePlan{
					wg:          wg,
					parentlines: make(chan *Line, 64),
					isConfig:    true,
				},
				oldToNew: make(map[string]string),
			}
			for _, part := range parts {
				part = TrimSpace(part)
				if strings.Contains(part, "(") {
					var funcName string
					var colNames []string
					if strings.Contains(part, "as") {
						split := strings.Split(part, "as")
						openIndex := strings.IndexByte(split[0], '(')
						closeIndex := strings.LastIndexByte(split[0], ')')
						funcName = split[0][:openIndex]
						args := split[0][openIndex+1 : closeIndex]
						colNames = strings.Split(args, ",")
						oldName := NewColName(funcName, colNames...)
						rnp.oldToNew[oldName] = split[1]
						outOuder = append(outOuder, split[1])
					} else {
						openIndex := strings.IndexByte(part, '(')
						closeIndex := strings.LastIndexByte(part, ')')
						funcName = part[:openIndex]
						args := part[openIndex+1 : closeIndex]
						colNames = strings.Split(args, ",")
						outOuder = append(outOuder, NewColName(funcName, colNames...))
					}
					if _, ok := t.db.ColFuncs[funcName]; ok {
						fcp := &FuncColPlan{
							basePlan: basePlan{
								wg:          wg,
								parentlines: make(chan *Line, 64),
								isConfig:    true,
							},
							colFuncs: make(map[string]func(any) any, 64),
						}
						fcp.colToFuncs = append(fcp.colToFuncs, struct {
							colName, funcName string
						}{colName: colNames[0], funcName: funcName})
						fcp.colFuncs[funcName] = t.db.ColFuncs[funcName]
						plans[FuncCol] = fcp
					}
					if _, ok := t.db.AggFuncs[funcName]; ok {
						agp.colToFuncs = append(agp.colToFuncs, struct {
							colName, funcName string
						}{colName: colNames[0], funcName: funcName})
						agp.aggFuncs[funcName] = t.db.AggFuncs[funcName]
					}
					if _, ok := t.db.ExecFuncs[funcName]; ok {
						etp := &ExecutePlan{
							basePlan: basePlan{
								wg:          wg,
								parentlines: make(chan *Line, 64),
								isConfig:    true,
							},
							execFuncs: make(map[string]func([]any) any, 64),
						}
						etp.colToFuncs = append(etp.colToFuncs, struct {
							colNames []string
							funcName string
						}{colNames: colNames, funcName: funcName})
						etp.execFuncs[funcName] = t.db.ExecFuncs[funcName]
						plans[Execute] = etp
					}
					for _, val := range colNames {
						pjp.colNames[val] = struct{}{}
					}
				} else if part == "*" {
					tableName := strings.TrimSpace(slice[i+3])
					table := t.db.tables[tableName]
					if table == nil {
						return nil, nil, "", errors.New("invalid table name")
					}
					for _, column := range table.Columns {
						pjp.colNames[column.Name] = struct{}{}
						outOuder = append(outOuder, column.Name)
					}
				} else {
					pjp.colNames[part] = struct{}{}
					outOuder = append(outOuder, part)
				}
			}
			if len(rnp.oldToNew) != 0 {
				plans[Rename] = rnp
			}
		case "from": //beta版本只支持单表操作
			tableName = strings.TrimSpace(slice[i+1])
			trp := &TableReadPlan{
				basePlan: basePlan{
					wg:          wg,
					parentlines: make(chan *Line, 64),
					isConfig:    true,
				},
				tx:        t,
				tableName: tableName,
			}
			plans[TableRead] = trp
		case "where":
			str := TrimSpace(slice[i+1])
			reg := regexp.MustCompile(`(?:[^,(]|\([^)]*\))+`)
			parts := reg.FindAllString(str, -1)
			slp := &SelectionPlan{
				basePlan: basePlan{
					wg:          wg,
					parentlines: make(chan *Line, 64),
					isConfig:    true,
				},
				selFuncs: make(map[string]func([]any) bool, 64),
			}
			for _, part := range parts {

				openIndex := strings.IndexByte(part, '(')
				closeIndex := strings.LastIndexByte(part, ')')
				funcName := part[:openIndex]
				args := part[openIndex+1 : closeIndex]
				colNames := strings.Split(args, ",")
				if _, ok := t.db.CondiFuncs[funcName]; ok {
					slp.colToFuncs = append(slp.colToFuncs, struct {
						colNames []string
						funcName string
					}{colNames: colNames, funcName: funcName})
					slp.selFuncs[funcName] = t.db.CondiFuncs[funcName]
					for _, val := range colNames {
						pjp.colNames[val] = struct{}{}
					}
				} else {
					return nil, nil, "", errors.New("invalid function name")
				}
			}
			plans[Selection] = slp
		case "group by":
			parts := strings.Split(slice[i+1], ",")
			colNames := make([]string, 0, 8)
			for _, part := range parts {
				val := strings.TrimSpace(part)
				colNames = append(colNames, val)
				pjp.colNames[val] = struct{}{}
			}
			agp.byCols = colNames
			agp.isConfig = true
			plans[Aggregation] = agp
		case "having":

			str := TrimSpace(slice[i+1])

			reg := regexp.MustCompile(`(?:[^,(]|\([^)]*\))+`)
			parts := reg.FindAllString(str, -1)

			hvp := &HavingPlan{
				basePlan: basePlan{
					wg:          wg,
					parentlines: make(chan *Line, 64),
				},
				havFuncs: make(map[string]func([]any) bool, 64),
			}

			for _, part := range parts {

				openIndex := strings.IndexByte(part, '(')
				closeIndex := strings.LastIndexByte(part, ')')
				funcName := part[:openIndex]
				args := part[openIndex+1 : closeIndex]
				colNames := strings.Split(args, ",")

				if _, ok := t.db.CondiFuncs[funcName]; ok {
					hvp.colToFuncs = append(hvp.colToFuncs, struct {
						colNames []string
						funcName string
					}{colNames: colNames, funcName: funcName})
					hvp.havFuncs[funcName] = t.db.CondiFuncs[funcName]
					hvp.isConfig = true
				}
			}
			plans[Having] = hvp
		case "order by":
			stp := &SortingPlan{
				basePlan: basePlan{
					wg:          wg,
					parentlines: make(chan *Line, 64),
					isConfig:    true,
				},
				colNames: make([]string, 0, 64),
			}
			var str string
			if strings.Contains(slice[i+1], "asc") {
				stp.isAsc = true
				str = "asc"
			} else if strings.Contains(slice[i+1], "desc") {
				stp.isAsc = false
				str = "desc"
			}
			split := strings.Split(slice[i+1], str)
			parts := strings.Split(split[0], ",")
			colNames := make([]string, 0, 8)
			for _, part := range parts {
				val := strings.TrimSpace(part)
				colNames = append(colNames, val)
				pjp.colNames[val] = struct{}{}
			}
			stp.colNames = colNames
			plans[Sorting] = stp
		case "limit":
			parts := strings.Split(slice[i+1], ",")
			if len(parts) != 2 {
				return nil, nil, "", errors.New("invalid args")
			}
			offset, err := strconv.Atoi(strings.TrimSpace(parts[0]))
			if err != nil {
				return nil, nil, "", errors.New("invalid offset")
			}
			count, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err != nil {
				return nil, nil, "", errors.New("invalid count")
			}
			ltp := &LimitPlan{
				basePlan: basePlan{
					wg:          wg,
					parentlines: make(chan *Line, 64),
					isConfig:    true,
				},
				offset: uint64(offset),
				count:  uint64(count),
			}
			plans[Limit] = ltp
		default:
		}
	}
	plans[Projection] = pjp
	return plans, outOuder, tableName, nil
}

func (t *Transaction) CompileUpdate(sql string) error {

	if strings.HasPrefix(sql, "insert") { //`insert into test (name, age , id,price ) values  ( "aaa", 12,8, 3.14)`

		slice := strings.SplitN(sql, "into", 2)
		str := strings.TrimSpace(slice[1])

		buf := new(bytes.Buffer)
		var tableEnd int
		for i := 0; i < len(str); i++ {
			if str[i] == 0x20 {
				tableEnd = i
				break
			}
			buf.WriteByte(str[i])
		}

		tableName := str[:tableEnd]
		table := t.db.tables[tableName]
		table.updated = true

		str = strings.TrimSpace(str[tableEnd:]) //`(name, age , id,price ) values  ( "aaa", 12,8, 3.14)`

		buf = new(bytes.Buffer)
		var valBegin int
		for i := 1; i < len(str); i++ { //从1开始跳过(
			if str[i] == 0x29 {
				valBegin = i + 1
				break
			}
			buf.WriteByte(str[i])
		}

		colStr := TrimSpace(buf.String()) //`name, age , id,price `
		columns := strings.Split(colStr, ",")

		str = str[valBegin:]
		slice = strings.SplitN(str, "values", 2)
		str = TrimSpace(slice[1])
		valStr := str[1 : len(str)-1] //` "aaa", 12,8, 3.14`
		valStr = TrimSpace(valStr)
		values := strings.Split(valStr, ",")
		if len(columns) != len(values) {
			return errors.New("column names and values cannot match")
		}

		colToVals := make(map[string]string, 64)
		for index, colName := range columns {
			colToVals[colName] = values[index]
		}
		line := Line{
			nameToVal: make(map[string]ColVal, 16),
		}
		for _, column := range table.Columns {
			if value, ok := colToVals[column.Name]; ok {
				line.nameToVal[column.Name] = ColVal{
					column: column,
					value:  []byte(value),
				}
			} else {
				defVal, err := defaultValue(column.TypeOf)
				if err != nil {
					return errors.New("get default value error")
				}
				line.nameToVal[column.Name] = ColVal{
					column: column,
					value:  defVal,
				}
			}
		}
		subTx := t.subTxs[tableName]
		memTable := subTx.memTables[0]
		lineId := uint64(len(memTable.lines))
		line.pageId = 0
		line.lineId = lineId
		memTable.lines[lineId] = line

	} else if strings.HasPrefix(sql, "update") { //`update  test set  name =" xxx" ,age= 9 where  nameEql(name)`

		slice := strings.SplitN(sql, "update", 2)
		str := strings.TrimSpace(slice[1]) //`test set  name =" xxx" ,age= 9 where  nameEql(name)`

		buf := new(bytes.Buffer)
		var tableEnd int
		for i := 0; i < len(str); i++ {
			if str[i] == 0x20 {
				tableEnd = i
				break
			}
			buf.WriteByte(str[i])
		}

		tableName := str[:tableEnd] //`test`
		table := t.db.tables[tableName]
		table.updated = true

		str = strings.TrimSpace(str[tableEnd:]) //`set  name =" xxx" ,age= 9 where  nameEql(name)`

		slice = strings.SplitN(str, "set", 2)
		str = strings.TrimSpace(slice[1]) //`name =" xxx" ,age= 9 where  nameEql(name)`

		slice = strings.SplitN(str, "where", 2)
		colValStr := TrimSpace(slice[0]) //`name="xxx",age=9`
		condition := TrimSpace(slice[1]) //`  nameEql(name)`

		query := make([]string, 0, 64)
		query = append(query, "select", "*", "from", tableName, "where", condition)
		var wg sync.WaitGroup
		logicalPlans, outOuder, _, err := t.CompileQuery(query, &wg)
		if err != nil {
			return err
		}
		resultSet := execute(logicalPlans, outOuder, &wg)

		colToVals := strings.Split(colValStr, ",")

		subTx := t.subTxs[tableName]
		table.cache.lock.Lock()
		for _, line := range resultSet.result {

			newLine := *line

			for _, colToVal := range colToVals {
				kv := strings.Split(colToVal, "=")
				column := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])

				newLine.nameToVal[column] = ColVal{
					column: line.nameToVal[column].column,
					value:  []byte(value),
				}

			}
			memTable := subTx.memTables[newLine.pageId]
			if memTable == nil {
				memTable, err = table.cache.CopyPage(newLine.pageId)
				if err != nil {
					return err
				}
				subTx.memTables[newLine.pageId] = memTable
			}
			memTable.lines[newLine.lineId] = newLine
			memTable.isOrigin = false

		}
		table.cache.lock.Unlock()
	} else if strings.HasPrefix(sql, "delete") {

		slice := strings.SplitN(sql, "from", 2) //`delete from  test where  othfloat( price) `
		str := strings.TrimSpace(slice[1])      //`test where  othfloat( price)`

		slice = strings.SplitN(str, "where", 2)

		tableName := strings.TrimSpace(slice[0]) //`test`
		table := t.db.tables[tableName]
		table.updated = true

		condition := strings.TrimSpace(slice[1]) //`othfloat( price)`

		query := make([]string, 0, 64)
		query = append(query, "select", "*", "from", tableName, "where", condition)
		var wg sync.WaitGroup
		logicalPlans, outOuder, _, err := t.CompileQuery(query, &wg)
		if err != nil {
			return err
		}
		resultSet := execute(logicalPlans, outOuder, &wg)

		subTx := t.subTxs[tableName]

		table.cache.lock.Lock()

		for _, line := range resultSet.result {
			memTable := subTx.memTables[line.pageId]
			if memTable == nil {
				memTable, err = table.cache.CopyPage(line.pageId)
				if err != nil {
					return err
				}
				subTx.memTables[line.pageId] = memTable
			}
			delete(memTable.lines, line.lineId)
			memTable.isOrigin = false
		}

		table.cache.lock.Unlock()

	} else {
		return errors.New("invalid sql")
	}
	return nil
}
