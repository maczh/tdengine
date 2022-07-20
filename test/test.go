package main

import (
	"fmt"
	"math/rand"
	"tdengine"
	"time"
)

type trafficRow struct {
	Timestamp time.Time `json:"time" td:"time"`
	Traffic   int64     `json:"traffic" td:"traffic"`
}

//func (t *trafficRow) GetMeter(field string) interface{} {
//	switch field {
//	case "time":
//		return t.Timestamp
//	case "traffic":
//		return t.Traffic
//	}
//	return 0
//}
//
//func (t *trafficRow) SetMeter(field string, value interface{}) {
//	switch field {
//	case "time":
//		t.Timestamp = value.(int64)
//	case "traffic":
//		t.Traffic = value.(int64)
//	}
//}

type traffic struct {
	Timestamp time.Time `json:"time" td:"time"`
	Traffic   int64     `json:"traffic" td:"traffic"`
}

func (t *traffic) GetMeter(field string) interface{} {
	switch field {
	case "time":
		return t.Timestamp
	case "traffic":
		return t.Traffic
	}
	return 0
}

func (t *traffic) SetMeter(field string, value interface{}) {
	switch field {
	case "time":
		t.Timestamp = value.(time.Time)
	case "traffic":
		t.Traffic = value.(int64)
	}
}

func main() {
	tdenginDsn := "root:TDengine#2022@tcp(tdengine-server:6030)/cdn_traffic"
	config := tdengine.Config{
		MaxIdelConns:    5,
		MaxOpenConns:    20,
		MaxIdelTimeout:  30,
		MaxConnLifetime: 300,
	}
	td, err := tdengine.New(tdenginDsn)
	if err != nil {
		fmt.Printf("TDengine.New: %v\n", err.Error())
	}
	defer td.Close()
	td = td.ConnPool(config)
	fmt.Println("插入测试,最近1小时，每5秒一条数据")
	now := time.Now().Add(-1 * time.Hour)
	rows := make([]trafficRow, 360)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 5; i++ {
		now = now.Add(10 * time.Second)
		rows[i].Timestamp = now
		rows[i].Traffic = r.Int63n(1024 * 1024 * 1024)
	}
	fmt.Println("单条插入测试，连续10条")
	stable := td.STable("cdn_traffic").Debug()
	table := stable.Table("test_com_downout")
	//for i := 0; i < 9; i++ {
	//	err = table.Tags([]interface{}{".test.com", "DownOut"}).
	//		Insert(&rows[i])
	//	if err != nil {
	//		fmt.Printf("单条%v --插入错误:%s\n", rows[i], err.Error())
	//	}
	//}
	fmt.Println("批量插入测试")
	err = table.InsertBatch(rows)
	if err != nil {
		fmt.Printf("批量插入错误:%s\n", err.Error())
	}
	//fmt.Println("指标直接查询测试")
	//var rs []traffic
	//err = stable.NewQuery().Table("test_com_downout").Fields([]string{"time", "traffic"}).Where("time > ?", "2022-07-19 21:51:21").OrderBy("time DESC").Limit(5).Find(&rs)
	//if err != nil {
	//	fmt.Printf("指标直接查询错误:%s\n", err.Error())
	//	return
	//}
	//fmt.Printf("查询结果:%v\n", rs)
	//fmt.Println("查询单条结果测试")
	//var row traffic
	//err = stable.NewQuery().Table("test_com_downout").Fields([]string{"time", "traffic"}).Where("time > ?", "2022-07-19 21:51:21").OrderBy("time DESC").FindOne(&row)
	//if err != nil {
	//	fmt.Printf("指标直接查询错误:%s\n", err.Error())
	//	return
	//}
	//fmt.Printf("查询结果:%v\n", row)
	//fmt.Println("指标5分钟汇总查询测试")
	//err = stable.NewQuery().Table("test_com_downout").Fields([]string{"sum(traffic) AS traffic"}).Where("time > ?", "2022-07-19 21:51:21").Interval("5m").OrderBy("time DESC").Find(&rs)
	//if err != nil {
	//	fmt.Printf("指标5分钟汇总查询错误:%s\n", err.Error())
	//	return
	//}
	//fmt.Printf("汇总查询结果:%v\n", rs)
}
