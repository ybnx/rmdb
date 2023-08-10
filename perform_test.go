package rmdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

var db *Database

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func GetName() string {
	var str bytes.Buffer
	for i := 0; i < 10; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return str.String()
}

func GetSql() string {
	name, _ := EncodeData(GetName())
	age, _ := EncodeData(int64(rand.Int()))
	id, _ := EncodeData(int64(rand.Int()))
	price, _ := EncodeData(rand.Float64())
	sql := fmt.Sprint("insert into test (name, age, id, price) values (", string(name), ", ", string(age), ", ", string(id), ", ", string(price), ");")
	return sql
}

func GetNumSql(num int) string {
	nameStr, _ := json.Marshal(fmt.Sprint("iam", num))
	ageStr, _ := json.Marshal(int64(num))
	idStr, _ := json.Marshal(int64(num + 1))
	priceStr, _ := json.Marshal(float64(num) * 0.01)
	sql := fmt.Sprintf("insert into test (name, age, id, price) values (%s, %s, %s, %s);", string(nameStr), string(ageStr), string(idStr), string(priceStr))
	return sql
}

func TestSql(t *testing.T) {
	fmt.Println(GetNumSql(11451))
}

func BenchmarkInsert(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()

	sql := GetSql()
	var err error
	for i := 0; i < b.N; i++ {
		err = db.Update(sql)
		assert.Nil(b, err)
	}

}
