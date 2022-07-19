package tdengine

import (
	"database/sql"
	"errors"
	"github.com/sadlil/gologger"
	_ "github.com/taosdata/driver-go/v2/taosSql"
	"time"
)

var logger = gologger.GetLogger()
type TDengine struct {
	Dsn          string  `json:"dsn"`
	DB           *sql.DB `json:"db"`
	DatabaseName string  `json:"database"`
}

type Config struct {
	MaxIdelConns    int
	MaxOpenConns    int
	MaxIdelTimeout  int
	MaxConnLifetime int
}

func New(dsn string) (TDengine, error) {
	tdengine := TDengine{
		Dsn: dsn,
	}
	err := tdengine.Connect()
	return tdengine, err
}

func (t TDengine) ConnPool(config Config) TDengine {
	if config.MaxIdelConns > 0 {
		t.SetMaxIdelConns(config.MaxIdelConns)
	}
	if config.MaxOpenConns > 0 {
		t.SetMaxOpenConns(config.MaxOpenConns)
	}
	if config.MaxIdelTimeout > 0 {
		t.SetIdleTimeout(time.Duration(config.MaxIdelTimeout) * time.Second)
	}
	if config.MaxConnLifetime > 0 {
		t.SetConnLifetime(time.Duration(config.MaxConnLifetime) * time.Second)
	}
	return t
}

func (t TDengine) Connect() error {
	taos, err := sql.Open("taosSql", t.Dsn)
	if err != nil {
		logger.Error("TDengine connect error:" + err.Error())
		return err
	}
	t.DB = taos
	return err
}

func (t TDengine) SetMaxIdelConns(max int) TDengine {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return t
	}
	t.DB.SetMaxIdleConns(max)
	return t
}

func (t TDengine) SetMaxOpenConns(max int) TDengine {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return t
	}
	t.DB.SetMaxOpenConns(max)
	return t
}

func (t TDengine) SetIdleTimeout(timeout time.Duration) TDengine {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return t
	}
	t.DB.SetConnMaxIdleTime(timeout)
	return t
}

func (t TDengine) Ping() error {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return errors.New("TDengine unconnected")
	}
	err := t.DB.Ping()
	if err != nil {
		logger.Error("TDengine connect error: " + err.Error())
	}
	return err
}

func (t TDengine) SetConnLifetime(timeout time.Duration) TDengine {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return t
	}
	t.DB.SetConnMaxLifetime(timeout)
	return t
}

func (t TDengine) Database(db string) TDengine {
	t.DatabaseName = db
	return t
}

func (t TDengine) Close() error {
	return t.DB.Close()
}

func (t TDengine) STable(stable string) Session {
	session := Session{
		tdengine: &t,
		Database: t.DatabaseName,
		SuperTable: stable,
	}
	return session
}
