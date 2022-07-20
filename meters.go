package tdengine

type Meters interface {
	GetMeter(field string) interface{}
	SetMeter(field string, value interface{})
}
