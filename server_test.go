package rmdb

import "testing"

func init() {
	GlobalOption.CondiFuncs["range"] = func(col []any) bool {
		return col[0].(string) > "como" && col[0].(string) < "zuzu"
	}
	GlobalOption.CondiFuncs["great_float"] = func(col []any) bool {
		return col[0].(float64) > 10
	}
	GlobalOption.CondiFuncs["gtfloat"] = func(col []any) bool {
		return col[0].(float64) > 7
	}
	GlobalOption.CondiFuncs["great"] = func(col []any) bool {
		return col[0].(int64) > 8
	}
	GlobalOption.CondiFuncs["kaguoka"] = func(col []any) bool {
		return col[0].(string) == "kaguoka"
	}
	GlobalOption.AggFuncs["sum"] = func(vals []any) any {
		sum := float64(0)
		for _, val := range vals {
			sum += val.(float64)
		}
		return sum
	}
	GlobalOption.ColFuncs["addstr"] = func(val any) any {
		return val.(string) + " is my name"
	}
	GlobalOption.ExecFuncs["product"] = func(cols []any) any {
		return cols[0].(int64) * cols[1].(int64)
	}
}

func TestServer(t *testing.T) {
	RunServer()
}

func TestClient(t *testing.T) {
	RunClient()
}
