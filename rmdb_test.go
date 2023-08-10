package rmdb

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTwo(t *testing.T) {
	t.Run("hello", TestCompile)
	t.Run("demo", TestCreateDB)
}

func TestCreateDB(t *testing.T) {
	db, err := CreateDatabase("demo")
	if err != nil {
		t.Fatal(err)
	}
	tab1, err := db.CreateTable("man")
	if err != nil {
		t.Fatal(err)
	}
	err = tab1.SetColumn("name", STRING)
	if err != nil {
		t.Fatal(err)
	}
	err = tab1.SetColumn("age", INT64)
	if err != nil {
		t.Fatal(err)
	}
	tab2, err := db.CreateTable("thing")
	if err != nil {
		t.Fatal(err)
	}
	err = tab2.SetColumn("id", INT64)
	if err != nil {
		t.Fatal(err)
	}
	err = tab2.SetColumn("price", FLOAT64)
	if err != nil {
		t.Fatal(err)
	}
	db.Update(`insert into man (name,age ) values ( "john",  8 );`)
	db.Update(` insert into man (name,age)  values ("chris", 9 );`)
	db.Update(`insert into thing  ( id ,price) values (0, 11.4) ; `)
	db.Update(`  insert into thing ( id ,price) values ( 1, 5.14);`)
	res1, _ := db.Query(` select * from  man`)
	fmt.Println(res1.ToString())
	res2, _ := db.Query(`select * from   thing`)
	fmt.Println(res2.ToString())
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestUseDB(t *testing.T) {
	db, err := UseDatabase("demo")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(db.ShowTables())
	res, _ := db.Query(` select * from  man`)
	fmt.Println(res.ToString())
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDropTable(t *testing.T) {
	db, err := UseDatabase("demo")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(db.ShowTables())
	err = db.DropTable("thing")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(db.ShowTables())
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDropDB(t *testing.T) {
	fmt.Println(ShowDatabase())
	err := DropDatabase("demo")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ShowDatabase())
}

func TestCompile(t *testing.T) {

	//GlobalOption.IOMode = MMapMode
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

	sql := `
		insert into test (name,age,  id,price) values ("ak47", 2, 4, 15.14);
insert into test (name, age,id,price) values ("como",3,4,3.14);
		   insert into test (  name, age,id,   price) values ("lala" ,  3, 4,  6.19);
		insert into test (name  , age,id ,price) values  ( "haha", 2 ,4 ,2.33)  ;
		  insert into test (name, age,id,price )   values ("momo",3,4,5.74);
		    insert into test (name, age,id,price )   values ( "kaguoka",5 ,1 , 6.66);
		insert into test (name, age,id,price )   values ("niuniu"  ,2,5,7.77);
	insert into   test (  name,age,id,price) values ("zuzu" ,6 ,4 ,9.99);
	`
	db.Update(sql)

	res, _ := db.Query("  select *  from test  where range(name  )") // TODO多个条件分and or

	fmt.Println(res.ToString())

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestConcur(t *testing.T) {

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
	wg.Add(5)
	go func() {
		defer wg.Done()
		db.Update(`insert into test (name,age,  id,price) values ("ak47", 2, 4, 15.14)`) //insert into和group by必须只有一个空格
		db.Update(`insert into test (name, age,id,price) values ("como",3,4,3.14)`)
	}()
	go func() {
		defer wg.Done()
		db.Update(`  insert into test (  name, age,id,   price) values ("lala" ,  3, 4,  6.19)`)
		db.Update(`insert into test (name  , age,id ,price) values  ( "haha", 2 ,4 ,2.33)  `)
	}()
	go func() {
		defer wg.Done()
		db.Update(`insert into test (name, age,id,price )   values ("momo",3,4,5.74)`)
		db.Update(`insert into test (name, age,id,price )   values ( "kaguoka",5 ,1 , 6.66)`)
	}()
	go func() {
		defer wg.Done()
		db.Update(`insert into test (name, age,id,price )   values ("niuniu"  ,2,5,7.77)`)
		db.Update(`  insert into   test (  name,age,id,price) values ("zuzu" ,6 ,4 ,9.99)`)
	}()
	time.Sleep(5 * time.Second)
	go func() {
		defer wg.Done()
		db.Update(`delete from  test where  gtfloat( price) `)
	}()
	wg.Wait()

	res, _ := db.Query("select * from test")

	fmt.Println(res.ToString())
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	//err = DropDatabase("hello")
	//if err != nil {
	//	t.Fatal(err)
	//}

}

func TestUsedb(t *testing.T) {

	GlobalOption.IOMode = MMapMode
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

	db, err := UseDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}

	res, _ := db.Query("select  * from test")

	fmt.Println(res.ToString())
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestSub(t *testing.T) {
	t.Run("create", TestCompile)
	t.Run("use", TestUsedb)
}

func TestRemoveDB(t *testing.T) {
	err := DropDatabase("hello")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMMap(t *testing.T) {
	file, err := NewMMap("E:\\golangProject\\demo2\\dbtest\\wal.txt", 1<<20)
	if err != nil {
		fmt.Println(err)
	}
	_, err = file.Write([]byte("abcdefghijklmn"))
	if err != nil {
		fmt.Println(err)
	}
	buf := make([]byte, 4)
	_, err = file.ReadAt(buf, 4)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	err = file.Close()
	if err != nil {
		fmt.Println(err)
	}
}

func TestEnDecode(t *testing.T) {
	a := true
	b := int64(56)
	c := float64(3.14)
	d := "mysql"
	e := time.Now()

	as, err := EncodeData(a)
	fmt.Println(as)
	if err != nil {
		t.Fatal(err)
	}
	bs, err := EncodeData(b)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(bs)
	cs, err := EncodeData(c)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cs)
	ds, err := EncodeData(d)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ds)
	es, err := EncodeData(e)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(es)

	val1, err := DecodeData(as, BOOL)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val1)

	val2, err := DecodeData(bs, INT64)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val2)

	val3, err := DecodeData(cs, FLOAT64)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val3)

	val4, err := DecodeData(ds, STRING)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val4)

	val5, err := DecodeData(es, DATE)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val5)

}

func TestInsert(t *testing.T) {
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
	err = table.SetColumn("price", FLOAT64)
	if err != nil {
		t.Fatal(err)
	}

	db.Update(`insert into test (name, age) values ("root", 1)`)
	res, _ := db.Query(`select * from test`)
	fmt.Println(res.ToString())

	db.Close()

}
