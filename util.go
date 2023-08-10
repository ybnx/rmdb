package rmdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"time"
)

func EncodeData(data any) ([]byte, error) {
	return json.Marshal(data)
}

func DecodeData(data []byte, typeOf int) (any, error) {
	switch typeOf {
	case BOOL:
		var dst bool
		err := json.Unmarshal(data, &dst)
		return dst, err
	case INT64:
		var dst int64
		err := json.Unmarshal(data, &dst)
		return dst, err
	case FLOAT64:
		var dst float64
		err := json.Unmarshal(data, &dst)
		return dst, err
	case STRING:
		var dst string
		err := json.Unmarshal(data, &dst)
		return dst, err
	case DATE:
		var dst time.Time
		err := json.Unmarshal(data, &dst)
		return dst, err
	default:
		return nil, nil
	}
}

func NewColName(funcName string, colNames ...string) string {
	buf := new(bytes.Buffer)
	buf.WriteString(funcName)
	buf.WriteString("(")
	buf.WriteString(colNames[0])
	for i := 1; i < len(colNames); i++ {
		buf.WriteString(",")
		buf.WriteString(colNames[i])
	}
	buf.WriteString(")")
	return buf.String()
}

func TrimSpace(str string) string { //去除全部空格
	buf := new(bytes.Buffer)
	for i := 0; i < len(str); i++ {
		if str[i] != 0x20 {
			buf.WriteByte(str[i])
		}
	}
	return buf.String()
}

func CheckParentheses(sql string) bool {
	left, right := 0, 0
	for i := 0; i < len(sql); i++ {
		if sql[i] == 0x28 {
			left++
		}
		if sql[i] == 0x29 {
			right++
		}
	}
	return left == right
}

func programInfo() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("Allocated memory: %d bytes\n", mem.Alloc)
	fmt.Printf("Total memory: %d bytes\n", mem.TotalAlloc)
	fmt.Printf("Number of mallocs: %d\n", mem.Mallocs)
	fmt.Printf("Number of frees: %d\n", mem.Frees)
	fmt.Println(runtime.NumGoroutine())
}
