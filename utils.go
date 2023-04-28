package tdengine

import (
	"bytes"
	j "encoding/json"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"reflect"
	"strings"
	"sync"
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

//	func toMap(value interface{}) map[string]interface{} {
//		obj := reflect.ValueOf(value)
//		result := make(map[string]interface{})
//		fieldCount := obj.NumField()
//		for i := 0; i < fieldCount; i++ {
//			f := obj.Field(i)
//			result[f.Tag.Get("json")] = f.
//		}
//	}
func getValByTag(refVal reflect.Value, refType reflect.Type, tag string) (interface{}, reflect.Kind, error) {
	typeCache.RLock()
	t, ok := typeCache.m[refType]
	typeCache.RUnlock()
	if !ok {
		fieldMap := make(map[string]int, refType.NumField())
		for i := 0; i < refType.NumField(); i++ {
			fieldMap[refType.Field(i).Tag.Get("td")] = i
		}
		typeCache.Lock()
		typeCache.m[refType] = fieldMap
		typeCache.Unlock()

		t = fieldMap
	}

	if index, ok := t[tag]; ok {
		field := refVal.Field(index)
		return field.Interface(), field.Type().Kind(), nil
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
	if v == nil {
		return nil
	}

	typeCache.RLock()
	t, ok := typeCache.m[refType]
	typeCache.RUnlock()

	if !ok {
		fieldMap := make(map[string]int, refType.NumField())
		for i := 0; i < refType.NumField(); i++ {
			fieldMap[refType.Field(i).Tag.Get("td")] = i
		}
		typeCache.Lock()
		typeCache.m[refType] = fieldMap
		typeCache.Unlock()

		t = fieldMap
	}

	if index, ok := t[tag]; ok {
		field := refVal.Field(index)
		if field.CanSet() {
			field.Set(reflect.ValueOf(v).Convert(field.Type()))
			return nil
		} else {
			return fmt.Errorf("field %v cannot be set", field)
		}
	}
	return errors.New("tag not found")
}

var typeCache = struct {
	sync.RWMutex
	m map[reflect.Type]map[string]int
}{m: make(map[reflect.Type]map[string]int)}

func setValueByTag(obj interface{}, tag string, v interface{}) error {
	refVal := reflect.ValueOf(obj).Elem()
	refType := reflect.TypeOf(obj).Elem()
	for i := 0; i < refVal.NumField(); i++ {
		field := refType.Field(i)
		if tag == field.Tag.Get("td") {
			refVal.Field(i).Set(reflect.ValueOf(v).Convert(field.Type))
			return nil
		}
	}
	return errors.New("tag not found")
}
