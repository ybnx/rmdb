package rmdb

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

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

func TestTx1(t *testing.T) {
	GlobalOption.IOMode = MMapMode
	db, err := CreateDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
	table, err := db.CreateTable("test")
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("name", STRING)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("age", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("id", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("price", FLOAT64)
	if err != nil {
		t.Fatal(err)
	}

	tx := db.Begin()
	err = tx.Update(`insert into test (name,age,  id,price) values ("ak47", 2, 4, 15.14)`)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	err = tx.Update(`insert into test (name, age,id,price) values ("como",3,4,3.14)`)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	err = tx.Update(`  insert into test (  name, age,id,   price) values ("lala" ,  3, 4,  6.19)`)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	err = tx.Update(`insert into test (name  , age,id ,price) values  ( "haha", 2 ,4 ,2.33)  `)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}


	res, err := db.Query(" select *  from test ;")
	if err != nil {
		t.Fatal(err)
	}


	fmt.Println(res.ToString())

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}



}

func TestTx2(t *testing.T) {

	db, err := CreateDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
	table, err := db.CreateTable("test")
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("name", STRING)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("age", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("id", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("price", FLOAT64)
	if err != nil {
		t.Fatal(err)
	}

	sqls := make([]string, 0, 16)
	sqls = append(sqls, `insert into test (name,age,  id,price) values ("ak47", 2, 4, 15.14)`)
	sqls = append(sqls, `insert into test (name, age,id,price) values ("como",3,4,3.14)`)
	sqls = append(sqls, `  insert into test (  name, age,id,   price) values ("lala" ,  3, 4,  6.19)`)
	sqls = append(sqls, `insert into test (name  , age,id ,price) values  ( "haha", 2 ,4 ,2.33)  `)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ("momo",3,4,5.74)`)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ( "kaguoka",5 ,1 , 6.66)`)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ("niuniu"  ,2,5,7.77)`)
	sqls = append(sqls, `  insert into   test (  name,age,id,price) values ("zuzu" ,6 ,4 ,9.99)`)

	tx := db.Begin()
	for _, sql := range sqls {
		err = tx.Update(sql)
		if err != nil {
			rb := tx.Rollback()
			if rb != nil {
				t.Error(rb)
			}
			t.Fatal(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		rb := tx.Rollback()
		if rb != nil {
			t.Error(rb)
		}
		t.Fatal(err)
	}



	res, err := db.Query(" select *  from test ;")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(res.ToString())

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxCur(t *testing.T) {

	db, err := CreateDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
	table, err := db.CreateTable("test")
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("name", STRING)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("age", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("id", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("price", FLOAT64)
	if err != nil {
		t.Fatal(err)
	}

	sqls := make([]string, 0, 16)
	sqls = append(sqls, `insert into test (name,age,  id,price) values ("ak47", 2, 4, 15.14)`)
	sqls = append(sqls, `insert into test (name, age,id,price) values ("como",3,4,3.14)`)
	sqls = append(sqls, `  insert into test (  name, age,id,   price) values ("lala" ,  3, 4,  6.19)`)
	sqls = append(sqls, `insert into test (name  , age,id ,price) values  ( "haha", 2 ,4 ,2.33)  `)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ("momo",3,4,5.74)`)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ( "kaguoka",5 ,1 , 6.66)`)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ("niuniu"  ,2,5,7.77)`)
	sqls = append(sqls, `  insert into   test (  name,age,id,price) values ("zuzu" ,6 ,4 ,9.99)`)

	var wg sync.WaitGroup
	wg.Add(len(sqls))

	for _, sql := range sqls {
		go func(sql string) {
			defer wg.Done()
			tx := db.Begin()
			err = tx.Update(sql)
			if err != nil {
				rb := tx.Rollback()
				if rb != nil {
					fmt.Println(rb)
				}
				fmt.Println(err)
			}
			err = tx.Commit()
			if err != nil {
				rb := tx.Rollback()
				if rb != nil {
					fmt.Println(rb)
				}
				fmt.Println(err)
			}
		}(sql)
	}

	wg.Wait()

	res, err := db.Query(" select *  from test ;")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(res.ToString())

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestUseDBase(t *testing.T) {
	db, err := UseDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(db.ShowTables())
	res, _ := db.Query(` select * from  test`)
	fmt.Println(res.ToString())
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleCur(t *testing.T) {

	db, err := CreateDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
	table, err := db.CreateTable("test")
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("name", STRING)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("age", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("id", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("price", FLOAT64)
	if err != nil {
		t.Fatal(err)
	}

	sqls := make([]string, 0, 16)
	sqls = append(sqls, `insert into test (name,age,  id,price) values ("ak47", 2, 4, 15.14)`)
	sqls = append(sqls, `insert into test (name, age,id,price) values ("como",3,4,3.14)`)
	sqls = append(sqls, `  insert into test (  name, age,id,   price) values ("lala" ,  3, 4,  6.19)`)
	sqls = append(sqls, `insert into test (name  , age,id ,price) values  ( "haha", 2 ,4 ,2.33)  `)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ("momo",3,4,5.74)`)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ( "kaguoka",5 ,1 , 6.66)`)
	sqls = append(sqls, `insert into test (name, age,id,price )   values ("niuniu"  ,2,5,7.77)`)
	sqls = append(sqls, `  insert into   test (  name,age,id,price) values ("zuzu" ,6 ,4 ,9.99)`)

	var wg sync.WaitGroup
	wg.Add(len(sqls))

	for _, sql := range sqls {
		go func(sql string) {
			defer wg.Done()
			err = db.Update(sql)
			if err != nil {
				fmt.Println(err)
			}
		}(sql)
	}

	wg.Wait()

	res, err := db.Query(" select *  from test ;")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(res.ToString())

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxSpeed(t *testing.T) {

	db, err := CreateDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
	table, err := db.CreateTable("test")
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("name", STRING)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("age", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("id", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("price", FLOAT64)
	if err != nil {
		t.Fatal(err)
	}

	tx := db.Begin()
	for i := 0; i < 10000; i++ {
		sql := GetSql()
		err = tx.Update(sql)
		if err != nil {
			rb := tx.Rollback()
			if rb != nil {
				t.Error(rb)
			}
			t.Fatal(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		rb := tx.Rollback()
		if rb != nil {
			t.Error(rb)
		}
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCurSpeed(t *testing.T) {

	//GlobalOption.IOMode = MMapMode
	db, err := CreateDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
	table, err := db.CreateTable("test")
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("name", STRING)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("age", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("id", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = table.SetColumn("price", FLOAT64)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(20000)

	for i := 0; i < 20000; i++ {
		go func(i int) {
			defer wg.Done()
			tx := db.Begin()
			sql := GetNumSql(i)
			err = tx.Update(sql)
			if err != nil {
				rb := tx.Rollback()
				if rb != nil {
					panic(rb)
				}
				panic(err)
			}
			err = tx.Commit()
			if err != nil {
				rb := tx.Rollback()
				if rb != nil {
					panic(rb)
				}
				panic(err)
			}
		}(i)
	}

	wg.Wait()
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCurCorrect(t *testing.T) {
	db, err := UseDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Query(` select * from  test`)
	assert.Nil(t, err)
	assert.Equal(t, 20000, len(res.result), "result should be 20000 line")
	for _, line := range res.result {
		nameData := line.nameToVal["name"].value
		ageData := line.nameToVal["age"].value
		idData := line.nameToVal["id"].value
		priceData := line.nameToVal["price"].value
		var num int
		var dst int64
		_ = json.Unmarshal(ageData, &dst)
		num = int(dst)
		nameStr, _ := json.Marshal(fmt.Sprint("iam", num))
		ageStr, _ := json.Marshal(int64(num))
		idStr, _ := json.Marshal(int64(num + 1))
		priceStr, _ := json.Marshal(float64(num) * 0.01)
		assert.Equal(t, string(nameStr), string(nameData), "name should equal")
		assert.Equal(t, string(ageStr), string(ageData), "age should equal")
		assert.Equal(t, string(idStr), string(idData), "id should equal")
		assert.Equal(t, string(priceStr), string(priceData), "price should equal")
	}
	err = db.Close()
	assert.Nil(t, err)
}

func TestDeleteAll(t *testing.T) {

	GlobalOption.CondiFuncs["strEql"] = func(vals []any) bool {
		return vals[0].(string) == "ak47" || vals[0].(string) == "como" || vals[0].(string) == "lala" || vals[0].(string) == "haha" || vals[0].(string) == "momo" || vals[0].(string) == "kaguoka" || vals[0].(string) == "niuniu" || vals[0].(string) == "zuzu"
	}
	db, err := UseDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}

	tx := db.Begin()
	err = tx.Update(`delete from  test where  strEql( name) ;`)
	if err != nil {
		rb := tx.Rollback()
		if rb != nil {
			t.Error(rb)
		}
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		rb := tx.Rollback()
		if rb != nil {
			t.Error(rb)
		}
		t.Fatal(err)
	}

	res, err := db.Query(" select *  from test ;")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(res.ToString())

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestRepeatedRead(t *testing.T) {

	db, err := UseDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}

	tx := db.Begin()

	assert.Nil(t, err)
	for i := 0; i < 10000; i++ {
		if i == 3141 {
			err := db.Update(`update  test set  name ="xxx" ,age= 9 where  kaguoka(name);`)
			assert.Nil(t, err)
		}

		res, err := tx.Query(`select *  from test where  kaguoka(name);`)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res.result), "result should be 1 line")
		line := res.result[0]
		nameData := line.nameToVal["name"].value
		ageData := line.nameToVal["age"].value
		idData := line.nameToVal["id"].value
		priceData := line.nameToVal["price"].value
		assert.Equal(t, "\"kaguoka\"", string(nameData), "name should equal")
		assert.Equal(t, "5", string(ageData), "age should equal")
		assert.Equal(t, "1", string(idData), "id should equal")
		assert.Equal(t, "6.66", string(priceData), "price should equal")

	}

	res, err := db.Query(`select *  from test where  kaguoka(name);`)
	assert.Equal(t, 0, len(res.result), "result should be 0 line")

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}


