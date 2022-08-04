package tdengine

import (
	"bytes"
	j "encoding/json"
	"errors"
	jsoniter "github.com/json-iterator/go"
	"reflect"
	"strings"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func toJSON(o interface{}) string {
	j, err := json.Marshal(o)
	if err != nil {
		return "{}"
	} else {
		js := string(j)
		js = strings.Replace(js, "\\u003c", "<", -1)
		js = strings.Replace(js, "\\u003e", ">", -1)
		js = strings.Replace(js, "\\u0026", "&", -1)
		return js
	}
}

func fromJSON(j string, o interface{}) *interface{} {
	err := json.Unmarshal([]byte(j), &o)
	if err != nil {
		logger.Error("数据转换错误:" + err.Error())
		return nil
	} else {
		return &o
	}
}

// JSONPrettyPrint pretty print raw json string to indent string
func JSONPretty(in, prefix, indent string) string {
	var out bytes.Buffer
	if err := j.Indent(&out, []byte(in), prefix, indent); err != nil {
		return in
	}
	return out.String()
}

// CompactJSON compact json input with insignificant space characters elided
func CompactJSON(in string) string {
	var out bytes.Buffer
	if err := j.Compact(&out, []byte(in)); err != nil {
		return in
	}
	return out.String()
}

//func toMap(value interface{}) map[string]interface{} {
//	obj := reflect.ValueOf(value)
//	result := make(map[string]interface{})
//	fieldCount := obj.NumField()
//	for i := 0; i < fieldCount; i++ {
//		f := obj.Field(i)
//		result[f.Tag.Get("json")] = f.
//	}
//}
func getValByTag(refVal reflect.Value, refType reflect.Type, tag string) (interface{}, reflect.Kind, error) {
	//refVal := reflect.ValueOf(obj).Elem()
	//refType := reflect.TypeOf(obj).Elem()
	for i := 0; i < refVal.NumField(); i++ {
		field := refType.Field(i)
		logger.Debug("field:" + field.Name + ",tag:" + tag + "tagval:" + field.Tag.Get("td"))
		if tag == field.Tag.Get("td") {
			return refVal.Field(i).Interface(), field.Type.Kind(), nil
		}
	}
	return nil, reflect.Int, errors.New("tag not found")
}

func getValueByTag(obj interface{}, tag string) (interface{}, reflect.Kind, error) {
	refVal := reflect.ValueOf(obj).Elem()
	refType := reflect.TypeOf(obj).Elem()
	for i := 0; i < refVal.NumField(); i++ {
		field := refType.Field(i)
		if tag == field.Tag.Get("td") {
			return refVal.Field(i).Interface(), field.Type.Kind(), nil
		}
	}
	return nil, reflect.Int, errors.New("tag not found")
}

func setValByTag(refVal reflect.Value, refType reflect.Type, tag string, v interface{}) error {
	//refVal := reflect.ValueOf(obj).Elem()
	//refType := reflect.TypeOf(obj).Elem()
	if v == nil {
		return nil
	}
	for i := 0; i < refVal.NumField(); i++ {
		field := refType.Field(i)
		if tag == field.Tag.Get("td") {
			refVal.Field(i).Set(reflect.ValueOf(v))
			return nil
		}
	}
	return errors.New("tag not found")
}

func setValueByTag(obj interface{}, tag string, v interface{}) error {
	refVal := reflect.ValueOf(obj).Elem()
	refType := reflect.TypeOf(obj).Elem()
	for i := 0; i < refVal.NumField(); i++ {
		field := refType.Field(i)
		if tag == field.Tag.Get("td") {
			refVal.Field(i).Set(reflect.ValueOf(v))
			return nil
		}
	}
	return errors.New("tag not found")
}
