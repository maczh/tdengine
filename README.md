# Go语言TDengin ORM框架

## 说明
- TDengine是国产优秀的时序数据库，比InfluxDB与Prometheus更易于使用，详见https://docs.taosdata.com/ 
- 本包是在TDengine go client基础上仿GORM进行了简单封装,实现了ORM映射。
- 使用本包需要事先安装TDengine Client包，目前仅支持Windows与Linux系统和macos 10.15以上版本。
- 使用本包需要配置TDengine服务器的域名，或者在客户端的/etc/hosts中配置服务器的主机名
- 本包v3版本仅支持TDengine v3.x版本，目前实测支持到v3.0.4版

## 目前支持的功能
- 单条数据插入
- 批量数据插入
- 查询返回多条结果
- 查询返回单条结果
- 子表自动建表

## ORM模型module
- 只针对指标字段建模，不支持TAGS字段建模
- 结构体的第一个字段必须是time.Time类型的TIMESTAMP字段
- 结构体中字段的tag必须有td标签，对应相应的指标字段名
- 结构体中字段的顺序必须与超级表中指标字段的顺序相同，字段类型相匹配
范例:
```go
type Traffic struct {
	Timestamp time.Time `json:"time" td:"time"`
	Traffic   int64     `json:"traffic" td:"traffic"`
}
```

## 对象与方法
### TDengine对象
- func New(dsn string) (*TDengine, error)   新建TDengine连接
- func (t *TDengine) ConnPool(config Config) *TDengine  设置逻辑池参数
- func (t *TDengine) Ping() error 测试连接
- func (t *TDengine) Database(db string) *TDengine  使用指定数据库
- func (t *TDengine) STable(stable string) *Session 使用指定超级表，生成Session对象
- func (t *TDengine) Close() error  关闭TDengine连接

### Session对象
- func (s *Session) NewQuery() *Session     新建查询，当使用同一个Session多次查询时，需要调用此函数进行重新初始化查询参数
- func (s *Session) Table(table string) *Session    指定子表名称
- func (s *Session) Tags(tags []interface{}) *Session   设置TAG标签字段值，按标签字段顺序每个字段都要设置。用于插入数据时自动创建子表
- func (s *Session) Meters(meters []string) *Session    设置标签字段名称，当不设置时将自动从表中获取
- func (s *Session) Debug() *Session    设置debug模式，自动在控制台输出SQL语句与返回结果
- func (s *Session) Insert(value interface{}) error     插入一条记录，value为指标模型对象
- func (s *Session) InsertBatch(values interface{}) error   批量插入多条数据，最多不超过10000条，values为指标模型对象数组
- func (s *Session) Fields(fields []string) *Session    设置SELECT查询返回的字段，不设置为\*，在字段中可以使用SQL函数如SUM(),AVG()等，参见TDengine函数，可以使用AS关键字设置字段别名
- func (s *Session) Where(query string, params ...interface{}) *Session  设置查询的WHERE条件，其中query可以使用?，用params中对应的参数自动替换
- func (s *Session) Interval(interval string) *Session  设置查询时间间隔，例如 10s/5m/1h，详见https://docs.taosdata.com/taos-sql/interval
- func (s *Session) OrderBy(orderBy string) *Session    设置结果排序ORDER BY
- func (s *Session) GroupBy(groupBy string) *Session    设置GROUP BY分组
- func (s *Session) Offset(offset int64) *Session       设置返回结果偏移量，用于分页
- func (s *Session) Limit(limit int) *Session           设置返回结果最大记录条数
- func (s *Session) Find(result interface{}) error      执行查询，返回多条记录，result是指标模型数组指针或[]map[string]interface{}
- func (s *Session) FindOne(result interface{}) error   执行查询，返回单条记录，result是指标模型结构体或map[string]interface{}
- func (s *Session) DescribeMeters() ([]string, error)  获取超级表的指标字段名数组
- func (s *Session) DescribeTags() ([]string, error)    获取超级表的标签字段名数组

## 使用范例
详见 test.go
### 导入包
```go
    import (
        "github.com/maczh/tdengine"
    )

```
### 建立连接
```go
	tdengineDsn := "user:password@tcp(tdengine-server:6030)/cdn_traffic"
	config := tdengine.Config{
		MaxIdelConns:    5,
		MaxOpenConns:    20,
		MaxIdelTimeout:  30,
		MaxConnLifetime: 300,
	}
	td, err := tdengine.New(tdengineDsn)
	if err != nil {
		fmt.Printf("TDengine.New: %v\n", err.Error())
	}
	defer td.Close()
	td = td.ConnPool(config)

```

### 插入单条数据
```go
    row := model.Traffic{
	    Timestamp: time.Now(),
	    Traffic: 123456,
    }
    stable := td.STable("test_traffic").Debug()
    table := stable.Table("test_table_tag1_tag2")
    err := table.Tags([]interface{}{"tag1", "tag2"}).Insert(row)
    if err != nil {
    	fmt.Printf("TDengine单条插入错误%s\n", err.Error())
    }
```

### 批量插入多条数据
```go
	now := time.Now().Add(-1 * time.Hour)
	rows := make([]model.Traffic, 10)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 10; i++ {
		now = now.Add(10 * time.Second)
		rows[i].Timestamp = now
		rows[i].Traffic = r.Int63n(1024 * 1024 * 1024)
	}
    stable := td.STable("test_traffic").Debug()
    table := stable.Table("test_table_tag1_tag2")
    err = table.InsertBatch(rows)
    if err != nil {
        fmt.Printf("批量插入错误:%s\n", err.Error())
    }

```

### 查询多条结果
```go
	var rs []model.Traffic
	err := stable.NewQuery().Table("test_table_tag1_tag2").Fields([]string{"time", "traffic"}).Where("time > ?", "2022-07-19 21:51:21").OrderBy("time DESC").Limit(5).Find(&rs)
	if err != nil {
		fmt.Printf("指标直接查询错误:%s\n", err.Error())
		return
	}
	fmt.Printf("查询结果:%v\n", rs)

```

### 分时段汇总查询
```go
	var rs []model.Traffic
	err := stable.NewQuery().Table("test_table_tag1_tag2").Fields([]string{"sum(traffic) AS traffic"}).Where("time > ?", "2022-07-19 21:51:21").Interval("5m").OrderBy("time DESC").Find(&rs)
	if err != nil {
		fmt.Printf("指标5分钟汇总查询错误:%s\n", err.Error())
		return
	}

```

### 单条结果查询
```go
	var row model.Traffic
	err = stable.NewQuery().Table("test_table_tag1_tag2").Fields([]string{"time", "traffic"}).Where("time > ?", "2022-07-19 21:51:21").OrderBy("time DESC").FindOne(&row)
	if err != nil {
		fmt.Printf("指标直接查询错误:%s\n", err.Error())
		return
	}
	fmt.Printf("查询结果:%v\n", row)

```