package tdengine

import (
	"fmt"
	"reflect"
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

func (c *Case) String() string {
	s := fmt.Sprintf(" CASE %s", c.Expr)
	for _, w := range c.Whens {
		switch reflect.TypeOf(w.When).Kind().String() {
		case "string":
			s += fmt.Sprintf(" WHEN '%s',", w.When)
		case "int", "int32", "int64":
			s += fmt.Sprintf(" WHEN %d,", w.When)
		case "float32", "float64":
			s += fmt.Sprintf(" WHEN %f,", w.When)
		default:
			s += fmt.Sprintf(" WHEN %f,", w.When)
		}
		switch reflect.TypeOf(w.Then).Kind().String() {
		case "string":
			s += fmt.Sprintf(" THEN '%s',", w.Then)
		case "int", "int32", "int64":
			s += fmt.Sprintf(" THEN %d,", w.Then)
		case "float32", "float64":
			s += fmt.Sprintf(" THEN %f,", w.Then)
		default:
			s += fmt.Sprintf(" THEN %f,", w.Then)
		}
	}
	switch reflect.TypeOf(c.ElseResult).Kind().String() {
	case "string":
		s += fmt.Sprintf(" ELSE '%s',", c.ElseResult)
	case "int", "int32", "int64":
		s += fmt.Sprintf(" ELSE %d,", c.ElseResult)
	case "float32", "float64":
		s += fmt.Sprintf(" ELSE %f,", c.ElseResult)
	default:
		s += fmt.Sprintf(" ELSE %f,", c.ElseResult)
	}

	return s
}
