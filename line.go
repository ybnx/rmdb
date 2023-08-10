package rmdb

import (
	"time"
)

type Line struct { //tuple代表一行记录
	nameToVal      map[string]ColVal
	pageId, lineId uint64
}

type ColVal struct {
	column Column
	value  []byte
}

type Column struct {
	Name   string
	TypeOf int
	DefVal []byte
}

const (
	BOOL = iota
	INT64
	FLOAT64
	STRING
	DATE
)

func CopyLine(line Line) Line { // TODO 好像可以直接 newLine := line
	newLine := Line{
		nameToVal: make(map[string]ColVal, 16),
	}
	for name, colVal := range line.nameToVal {
		newLine.nameToVal[name] = ColVal{
			column: colVal.column,
			value:  colVal.value,
		}
	}
	newLine.pageId = line.pageId
	newLine.lineId = line.lineId
	return newLine
}

func defaultValue(typeOf int) ([]byte, error) {
	switch typeOf {
	case BOOL:
		return EncodeData(false)
	case INT64:
		return EncodeData(0)
	case FLOAT64:
		return EncodeData(0)
	case STRING:
		return EncodeData("")
	case DATE:
		return EncodeData(new(time.Time))
	default:
		return nil, nil
	}
}

func GetTypeOf(value any) int {
	switch value.(type) {
	case bool:
		return BOOL
	case int64:
		return INT64
	case float64:
		return FLOAT64
	case string:
		return STRING
	case time.Time:
		return DATE
	default:
		return -1
	}
}
