package tdengine

import (
	"errors"
	"fmt"
	"strings"
)

type Session struct {
	tdengine   *TDengine
	Database   string `json:"database"`
	SuperTable string `json:"stable"`
	table      string
	action     string
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

func (s Session) Table(table string) Session {
	s.table = table
	return s
}

func (s Session) Tags(tags []interface{}) Session {
	s.tags = tags
	return s
}

func (s Session) Meters(meters []string) Session {
	s.meters = meters
	return s
}

func (s Session) Debug() Session {
	s.debug = true
	return s
}

func (s Session) Insert(value interface{}) error {
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
	val := map[string]interface{}{}
	FromJSON(ToJSON(value), &val)
	for _, meter := range s.meters {
		v := val[meter]
		switch v.(type) {
		case string:
			vals += fmt.Sprintf("'%s',", v.(string))
		case int, int64:
			vals += fmt.Sprintf("%d,", v.(int64))
		case float32, float64:
			vals += fmt.Sprintf("%f,", v.(float64))
		}
		vals = vals[:len(vals)-1]
		vals += ")"
	}
	strSql := ""
	if s.tags != nil && len(s.tags) > 0 {
		tags := ""
		for _, tag := range s.tags {
			switch tag.(type) {
			case string:
				tags += fmt.Sprintf("'%s',", tag.(string))
			case int, int64:
				tags += fmt.Sprintf("%d,", tag.(int64))
			case float32, float64:
				tags += fmt.Sprintf("%f,", tag.(float64))
			}
		}
		tags = tags[:len(tags)-1]
		strSql = fmt.Sprintf("INSERT INTO %s USING %s TAGS (%s) VALUES %s;", s.table, s.SuperTable, tags, vals)
	} else {
		strSql = fmt.Sprintf("INSERT INTO %s VALUES (%s);", s.table, vals)
	}
	if s.debug {
		logger.Debug(strSql)
	}
	_, err = s.tdengine.DB.Exec(strSql)
	return err
}

func (s Session) InsertBatch(values []interface{}) error {
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
	vals := ""
	val := []map[string]interface{}{}
	FromJSON(ToJSON(values), &val)
	for _, row := range val {
		vals += "("
		for _, meter := range s.meters {
			v := row[meter]
			switch v.(type) {
			case string:
				vals += fmt.Sprintf("'%s',", v.(string))
			case int, int64:
				vals += fmt.Sprintf("%d,", v.(int64))
			case float32, float64:
				vals += fmt.Sprintf("%f,", v.(float64))
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
			case int, int64:
				tags += fmt.Sprintf("%d,", tag.(int64))
			case float32, float64:
				tags += fmt.Sprintf("%f,", tag.(float64))
			}
		}
		tags = tags[:len(tags)-1]
		strSql = fmt.Sprintf("INSERT INTO %s USING %s TAGS (%s) VALUES %s;", s.table, s.SuperTable, tags, vals)
	} else {
		strSql = fmt.Sprintf("INSERT INTO %s VALUES (%s);", s.table, vals)
	}
	if s.debug {
		logger.Debug(strSql)
	}
	_, err = s.tdengine.DB.Exec(strSql)
	return err
}

func (s Session) OrderBy(orderBy string) Session {
	s.orderBy = orderBy
	return s
}

func (s Session) GroupBy(groupBy string) Session {
	s.groupBy = groupBy
	return s
}

func (s Session) Interval(interval string) Session {
	s.interval = interval
	return s
}

func (s Session) Offset(offset int64) Session{
	s.offset = offset
	return s
}

func (s Session) Limit(limit int) Session {
	s.limit = limit
	return s
}

func (s Session) Where(query string, params ...interface{}) Session {
	for _,param := range params {
		switch param.(type) {
		case string:
			query = strings.Replace(query,"?", fmt.Sprintf("'%s'",param.(string)),1)
		case int, int64:
			query = strings.Replace(query,"?", fmt.Sprintf("%d",param.(int64)),1)
		case float32, float64:
			query = strings.Replace(query,"?", fmt.Sprintf("%s",param.(float64)),1)
		case []interface{},[]int,[]int64,[]string,[]float64,[]float32:
			array := ToJSON(param)
			array = strings.Replace(strings.Replace(array,"[","",1),"]","",1)
			query = strings.Replace(query,"?",array,1)
		}
	}
	s.where = query
	return s
}

func (s Session) generateQuerySql() string {

}


func (s Session) DescribeMeters() ([]string, error) {
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

func (s Session) DescribeTags() ([]string, error) {
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
