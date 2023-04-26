package tdengine

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Case struct {
	Expr       string `json:"expr"`
	Whens      []When `json:"whens"`
	ElseResult any    `json:"else"`
}

type When struct {
	When any `json:"when"`
	Then any `json:"then"`
}

func NewCase() *Case {
	return &Case{}
}

func (c *Case) Case(expr string) *Case {
	c.Expr = expr
	return c
}

func (c *Case) When(when, then any) *Case {
	if c.Whens == nil {
		c.Whens = make([]When, 0)
	}
	c.Whens = append(c.Whens, When{
		When: when,
		Then: then,
	})
	return c
}
func (c *Case) Else(result any) *Case {
	c.ElseResult = result
	return c
}

func (c *Case) ToString() string {
	s := fmt.Sprintf(" CASE %s", c.Expr)
	for _, w := range c.Whens {
		s += fmt.Sprintf(" WHEN %s THEN %s", anyToString(w.When), anyToString(w.Then))
	}
	s += fmt.Sprintf(" ELSE %s END ", anyToString(c.ElseResult))
	return s
}

func anyToString(val any) string {
	s := ""
	switch reflect.TypeOf(val).Kind().String() {
	case "struct": //专门针对时间类型
		if t, ok := val.(time.Time); ok {
			s = fmt.Sprintf("%d", t.UnixMilli())
		} else {
			s = fmt.Sprintf("'%s'", toJSON(val))
		}
	case "map":
		s = fmt.Sprintf("'%s'", toJSON(val))
	case "slice", "array":
		array := toJSON(val)
		s = strings.Replace(strings.Replace(array, "[", "", 1), "]", "", 1)
	case "string":
		s = fmt.Sprintf("'%s'", val)
	case "int", "int32", "int64", "uint", "uint32", "uint64":
		s = fmt.Sprintf("%d", val)
	case "float32", "float64":
		s = fmt.Sprintf("%f", val)
	default:
		s = fmt.Sprintf("%v", val)
	}
	return s
}
