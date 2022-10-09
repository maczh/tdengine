package tdengine

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/sadlil/gologger"
	_ "github.com/taosdata/driver-go/v2/taosRestful"
	_ "github.com/taosdata/driver-go/v2/taosSql"
	"strings"
	"time"
)

var logger = gologger.GetLogger()

type TDengine struct {
	Dsn          string  `json:"dsn"`
	DB           *sql.DB `json:"db"`
	DatabaseName string  `json:"database"`
	Debug        bool    `json:"debug"`
	Type         string  `json:"type"`
}

type Config struct {
	MaxIdelConns    int
	MaxOpenConns    int
	MaxIdelTimeout  int
	MaxConnLifetime int
}

func New(dsn string) (*TDengine, error) {
	tdengine := TDengine{
		Dsn:  dsn,
		Type: "taosSql",
	}
	if strings.Contains(dsn, "@http") {
		tdengine.Type = "taosRestful"
	}
	err := tdengine.connect()
	return &tdengine, err
}

func (t *TDengine) ConnPool(config Config) *TDengine {
	if config.MaxIdelConns > 0 {
		t.setMaxIdelConns(config.MaxIdelConns)
	}
	if config.MaxOpenConns > 0 {
		t.setMaxOpenConns(config.MaxOpenConns)
	}
	if config.MaxIdelTimeout > 0 {
		t.setIdleTimeout(time.Duration(config.MaxIdelTimeout) * time.Second)
	}
	if config.MaxConnLifetime > 0 {
		t.setConnLifetime(time.Duration(config.MaxConnLifetime) * time.Second)
	}
	return t
}

func (t *TDengine) connect() error {
	var err error
	t.DB, err = sql.Open(t.Type, t.Dsn)
	if err != nil {
		logger.Error("TDengine connect error:" + err.Error())
		return err
	}
	return nil
}

func (t *TDengine) setMaxIdelConns(max int) *TDengine {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return t
	}
	t.DB.SetMaxIdleConns(max)
	return t
}

func (t *TDengine) setMaxOpenConns(max int) *TDengine {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return t
	}
	t.DB.SetMaxOpenConns(max)
	return t
}

func (t *TDengine) setIdleTimeout(timeout time.Duration) *TDengine {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return t
	}
	t.DB.SetConnMaxIdleTime(timeout)
	return t
}

func (t *TDengine) SetDebug() *TDengine {
	t.Debug = true
	return t
}

func (t *TDengine) Ping() error {
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

func (t *TDengine) setConnLifetime(timeout time.Duration) *TDengine {
	if t.DB == nil {
		logger.Error("TDengine connect first")
		return t
	}
	t.DB.SetConnMaxLifetime(timeout)
	return t
}

func (t *TDengine) Database(db string) *TDengine {
	t.DatabaseName = db
	_, err := t.DB.Exec(fmt.Sprintf("USE %s;", t.DatabaseName))
	if err != nil {
		logger.Error("TDengine database " + db + " not found: " + err.Error())
	}
	return t
}

func (t *TDengine) Close() error {
	return t.DB.Close()
}

func (t *TDengine) STable(stable string) *Session {
	session := Session{
		tdengine:   t,
		Database:   t.DatabaseName,
		SuperTable: stable,
	}
	if t.Type == "taosRestful" {
		session.SuperTable = fmt.Sprintf("%s.%s", t.DatabaseName, stable)
	}
	if t.Debug {
		return session.Debug()
	}
	return &session
}
