package tdengine

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Session struct {
	tdengine   *TDengine
	Database   string `json:"database"`
	SuperTable string `json:"stable"`
	table      string
	meters     []string      //指标名
	tags       []interface{} //标签值
	fields     []string
	where      string
	orderBy    string
	groupBy    string
	interval   string
	offset     int64
	limit      int
	debug      bool
}

func (s *Session) DebugInfo() {
	logger.Debug("Session: " + toJSON(s))
	logger.Debug("tdengine: " + toJSON(s.tdengine))
	logger.Debug("sql.db:" + toJSON(s.tdengine.DB.Stats()))
	logger.Debug("table:" + s.table)
}

func (s *Session) NewQuery() *Session {
	s.table = ""
	s.where = ""
	s.fields = []string{}
	s.interval = ""
	s.orderBy = ""
	s.groupBy = ""
	s.limit = 0
	s.offset = 0
	return s
}

func (s *Session) Table(table string) *Session {
	s.table = table
	return s
}

func (s *Session) Tags(tags ...interface{}) *Session {
	s.tags = tags
	return s
}

func (s *Session) Meters(meters ...string) *Session {
	s.meters = meters
	return s
}

func (s *Session) Debug() *Session {
	s.debug = true
	return s
}

func (s *Session) Fields(fields ...string) *Session {
	s.fields = fields
	return s
}

func (s *Session) Insert(value interface{}) error {
	if s.table == "" {
		return errors.New("Table name unseted")
	}
	var err error
	if s.meters == nil || len(s.meters) == 0 {
		s.meters, err = s.DescribeMeters()
		if err != nil {
			return err
		}
	}
	vals := "("
	refVal := reflect.ValueOf(value)
	refType := reflect.TypeOf(value)
	if refType.Kind() == reflect.Ptr {
		refVal = refVal.Elem()
		refType = refType.Elem()
	}
	for _, meter := range s.meters {
		v, k, err := getValByTag(refVal, refType, meter)
		if err != nil {
			logger.Error(err.Error())
			vals += "null,"
		}
		switch k.String() {
		case "struct": //专门针对时间类型
			if t, ok := v.(time.Time); ok {
				vals += fmt.Sprintf("%d,", t.UnixMilli())
			} else {
				vals += fmt.Sprintf("'%s',", toJSON(v))
			}
		case "string":
			vals += fmt.Sprintf("'%s',", v)
		case "int", "int32", "int64":
			vals += fmt.Sprintf("%d,", v)
		case "float32", "float64":
			vals += fmt.Sprintf("%f,", v)
		default:
			vals += fmt.Sprintf("%v,", v)
		}
	}
	vals = vals[:len(vals)-1]
	vals += ")"
	strSql := ""
	if s.tags != nil && len(s.tags) > 0 {
		tags := ""
		for _, tag := range s.tags {
			tobj := reflect.ValueOf(tag)
			switch tobj.Kind().String() {
			case "string":
				tags += fmt.Sprintf("'%s',", tag.(string))
			case "int64", "int", "int32":
				tags += fmt.Sprintf("%d,", tag.(int64))
			case "float32", "float64":
				tags += fmt.Sprintf("%f,", tag.(float64))
			}
		}
		tags = tags[:len(tags)-1]
		strSql = fmt.Sprintf("INSERT INTO %s USING %s TAGS (%s) VALUES %s;", s.table, s.SuperTable, tags, vals)
	} else {
		strSql = fmt.Sprintf("INSERT INTO %s VALUES %s;", s.table, vals)
	}
	//if s.debug {
	logger.Debug(strSql)
	//}
	_, err = s.tdengine.DB.Exec(strSql)
	return err
}

func (s *Session) InsertBatch(values interface{}) error {
	if s.table == "" {
		return errors.New("Table name unseted")
	}
	if values == nil {
		return errors.New("values is nil")
	}
	var err error
	if s.meters == nil || len(s.meters) == 0 {
		s.meters, err = s.DescribeMeters()
		if err != nil {
			return err
		}
	}
	if reflect.TypeOf(values).Kind() != reflect.Slice {
		return errors.New("values is not a slice")
	}
	refValues := reflect.ValueOf(values)
	if refValues.Len() == 0 {
		return errors.New("values is empty")
	}
	vals := ""
	for i := 0; i < refValues.Len(); i++ {
		value := refValues.Index(i)
		vals += "("
		for _, meter := range s.meters {
			v, k, err := getValByTag(value, value.Type(), meter)
			if err != nil {
				logger.Error(meter + " " + err.Error())
				vals += "null,"
			}
			switch k.String() {
			case "struct": //专门针对时间类型
				if t, ok := v.(time.Time); ok {
					vals += fmt.Sprintf("%d,", t.UnixMilli())
				} else {
					vals += fmt.Sprintf("'%s',", toJSON(v))
				}
			case "string":
				vals += fmt.Sprintf("'%s',", v)
			case "int", "int32", "int64":
				vals += fmt.Sprintf("%d,", v)
			case "float32", "float64":
				vals += fmt.Sprintf("%f,", v)
			default:
				vals += fmt.Sprintf("%v,", v)
			}
		}
		vals = vals[:len(vals)-1]
		vals += ") "
	}
	strSql := ""
	if s.tags != nil && len(s.tags) > 0 {
		tags := ""
		for _, tag := range s.tags {
			switch tag.(type) {
			case string:
				tags += fmt.Sprintf("'%s',", tag.(string))
			case float32, float64:
				if _, ok := tag.(int64); ok {
					tags += fmt.Sprintf("%d,", tag.(int64))
				} else {
					tags += fmt.Sprintf("%f,", tag.(float64))
				}
			}
		}
		tags = tags[:len(tags)-1]
		strSql = fmt.Sprintf("INSERT INTO %s USING %s TAGS (%s) VALUES %s;", s.table, s.SuperTable, tags, vals)
	} else {
		strSql = fmt.Sprintf("INSERT INTO %s VALUES %s;", s.table, vals)
	}
	if s.debug {
		logger.Debug(strSql)
	}
	_, err = s.tdengine.DB.Exec(strSql)
	return err
}

func (s *Session) OrderBy(orderBy string) *Session {
	s.orderBy = orderBy
	return s
}

func (s *Session) GroupBy(groupBy string) *Session {
	s.groupBy = groupBy
	return s
}

func (s *Session) Interval(interval string) *Session {
	s.interval = interval
	return s
}

func (s *Session) Offset(offset int64) *Session {
	s.offset = offset
	return s
}

func (s *Session) Limit(limit int) *Session {
	s.limit = limit
	return s
}

func (s *Session) Where(query string, params ...interface{}) *Session {
	for _, param := range params {
		switch param.(type) {
		case string:
			query = strings.Replace(query, "?", fmt.Sprintf("'%s'", param.(string)), 1)
		case int, int32, int64:
			query = strings.Replace(query, "?", fmt.Sprintf("%d", param.(int64)), 1)
		case float32, float64:
			query = strings.Replace(query, "?", fmt.Sprintf("%s", param.(float64)), 1)
		case []interface{}, []int, []int64, []string, []float64, []float32:
			array := toJSON(param)
			array = strings.Replace(strings.Replace(array, "[", "", 1), "]", "", 1)
			query = strings.Replace(query, "?", array, 1)
		}
	}
	s.where = query
	return s
}

func (s *Session) generateQuerySql() string {
	fields := "*"
	if s.fields != nil && len(s.fields) > 0 {
		fields = strings.Join(s.fields, ",")
	}
	table := s.SuperTable
	if s.table != "" {
		table = s.table
	}
	where := ""
	if s.where != "" {
		where = " WHERE " + s.where
	}
	interval := ""
	if s.interval != "" {
		interval = " INTERVAL(" + s.interval + ")"
	}
	groupBy := ""
	if s.groupBy != "" {
		groupBy = " GROUP BY " + s.groupBy
	}
	orderBy := ""
	if s.orderBy != "" {
		orderBy = " ORDER BY " + s.orderBy
	}
	limit := ""
	if s.limit != 0 {
		limit = fmt.Sprintf(" LIMIT %d", s.limit)
		if s.offset != 0 {
			limit += fmt.Sprintf(" OFFSET %d", s.offset)
		}
	}
	querySql := fmt.Sprintf("SELECT %s FROM %s %s %s %s %s %s", fields, table, where, interval, groupBy, orderBy, limit)
	querySql = strings.TrimRight(querySql, " ") + ";"
	return querySql
}

func (s *Session) Find(result interface{}) error {
	if result == nil {
		return errors.New("result is nil")
	}
	rs := reflect.ValueOf(result)
	if rs.Type().Kind() != reflect.Ptr {
		return errors.New("result is not a pointer")
	}
	rsRow := reflect.Indirect(rs)
	rsRowType := rsRow.Type().Elem()
	if rsRow.Type().Kind() != reflect.Slice {
		return errors.New("result is not a pointer of struct")
	}
	query := s.generateQuerySql()
	if s.debug {
		logger.Debug(query)
	}
	rows, err := s.tdengine.DB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	fields, _ := rows.Columns()
	fieldCount := len(fields)
	rowV := make([]interface{}, fieldCount)
	rowValue := make([]interface{}, fieldCount)
	for i := 0; i < fieldCount; i++ {
		rowValue[i] = &rowV[i]
	}
	for rows.Next() {
		row := reflect.New(rsRowType)
		if rsRowType.Kind() == reflect.Map {
			row = reflect.MakeMap(rsRowType)
		}
		err = rows.Scan(rowValue...)
		if err != nil {
			logger.Error("数据提取错误:" + err.Error())
		}
		var r reflect.Value
		for i, val := range rowV {
			switch rsRowType.Kind() {
			case reflect.Map:
				row.SetMapIndex(reflect.ValueOf(fields[i]), reflect.ValueOf(val))
				r = row
			case reflect.Struct:
				err = setValByTag(row.Elem(), rsRowType, fields[i], val)
				if err != nil {
					logger.Error("setValByTag:" + err.Error())
				}
				r = row.Elem()
			default:
				logger.Error("upsupport result type,must be *[]struct or *[]map[string]interface{}")
			}
		}
		rsRow.Set(reflect.Append(rsRow, r))
	}
	if s.debug {
		//fmt.Printf("TDengin return: %v\n", rsMapList)
		logger.Debug(toJSON(result))
	}
	return err
}

func (s *Session) FindOne(result interface{}) error {
	s.Limit(1)
	if result == nil {
		return errors.New("result is nil")
	}
	rs := reflect.ValueOf(result)
	if rs.Type().Kind() != reflect.Ptr {
		return errors.New("result is not a pointer")
	}
	rsRow := reflect.Indirect(rs)
	rsRowType := rs.Type().Elem()
	//fmt.Printf("rs.Type().Kind()=%v,rsRowType.Kind()=%v\n",rs.Type().Kind(),rsRowType.Kind())
	if rsRowType.Kind() != reflect.Struct && rsRowType.Kind() != reflect.Map {
		return errors.New("result is not a pointer of struct or map")
	}
	query := s.generateQuerySql()
	if s.debug {
		logger.Debug(query)
	}
	rows, err := s.tdengine.DB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	fields, _ := rows.Columns()
	fieldCount := len(fields)
	rowV := make([]interface{}, fieldCount)
	rowValue := make([]interface{}, fieldCount)
	for i := 0; i < fieldCount; i++ {
		rowValue[i] = &rowV[i]
	}
	for rows.Next() {
		row := reflect.New(rsRowType)
		if rsRowType.Kind() == reflect.Map {
			row = reflect.MakeMap(rsRowType)
		}
		err = rows.Scan(rowValue...)
		if err != nil {
			logger.Error("数据提取错误:" + err.Error())
		}
		var r reflect.Value
		for i, val := range rowV {
			switch rsRowType.Kind() {
			case reflect.Map:
				row.SetMapIndex(reflect.ValueOf(fields[i]), reflect.ValueOf(val))
				r = row
			case reflect.Struct:
				err = setValByTag(row.Elem(), rsRowType, fields[i], val)
				if err != nil {
					logger.Error("setValByTag:" + err.Error())
				}
				r = row.Elem()
			default:
				logger.Error("upsupport result type,must be *[]struct or *[]map[string]interface{}")
			}
		}
		rsRow.Set(r)
	}
	if s.debug {
		logger.Debug(toJSON(result))
	}
	return err
}

func (s *Session) DescribeMeters() ([]string, error) {
	strSql := fmt.Sprintf("DESCRIBE %s;", s.SuperTable)
	rows, err := s.tdengine.DB.Query(strSql)
	if err != nil {
		return nil, err
	}
	meters := make([]string, 0)
	for rows.Next() {
		var field, fieldType, note string
		var length int
		err = rows.Scan(&field, &fieldType, &length, &note)
		if err != nil {
			return nil, err
		}
		if note == "" {
			meters = append(meters, field)
		}
	}
	return meters, nil
}

func (s *Session) DescribeTags() ([]string, error) {
	strSql := fmt.Sprintf("DESCRIBE %s;", s.SuperTable)
	rows, err := s.tdengine.DB.Query(strSql)
	if err != nil {
		return nil, err
	}
	tags := make([]string, 0)
	for rows.Next() {
		var field, fieldType, note string
		var length int
		err = rows.Scan(&field, &fieldType, &length, &note)
		if err != nil {
			return nil, err
		}
		if note == "TAG" {
			tags = append(tags, field)
		}
	}
	return tags, nil
}
