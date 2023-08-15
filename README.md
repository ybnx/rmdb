# rmdb
关系型数据库可并发的对数据库进行增删改查操作，支持命令行交互，通过互斥锁实现可重复读隔离级别的事务，适合一致性要求强的业务场景，可以选择普通fileio和mmap两种读写模式，可自定义比较，聚合，对单列计算和多列计算的函数，通过buffer pool管理数据页，查询时通过将sql转化为执行计划进行查询

```go
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
```