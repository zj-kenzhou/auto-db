package datasource

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"
	"strings"

	"github.com/zj-kenzhou/gorm-oracle-ora"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

const _primary = "primary"
const _txKey = "sqlTx"
const _nameKey = "datasourceName"

var _dbMap = make(map[string]*gorm.DB)

var _configMap = make(map[string]Config)

var ErrUsernameNotFound = errors.New("datasource username is empty")

var ErrDatasourceNotFound = errors.New("datasource not found")

func Db() *gorm.DB {
	return _dbMap[_primary]
}

func GetDb(datasourceName string) *gorm.DB {
	return _dbMap[datasourceName]
}

func ToSqlTx(db *gorm.DB) *sql.Tx {
	return db.Statement.ConnPool.(*sql.Tx)
}

func TransactionWithDsName(ctx context.Context, datasourceName string, f func(txCtx context.Context) error) error {
	db, found := _dbMap[datasourceName]
	if !found {
		return ErrDatasourceNotFound
	}
	return db.Transaction(func(tx *gorm.DB) error {
		txCtx := context.WithValue(ctx, _txKey, ToSqlTx(tx))
		txCtx = context.WithValue(txCtx, _nameKey, datasourceName)
		return f(txCtx)
	})
}

func Transaction(ctx context.Context, f func(txCtx context.Context) error) error {
	return TransactionWithDsName(ctx, _primary, f)
}

func GetDbByCtx(ctx context.Context) *gorm.DB {
	return GetDbByCtxAndName(ctx, _primary)
}

func GetDbByCtxAndName(ctx context.Context, name string) *gorm.DB {
	if ctx.Value(_txKey) == nil {
		return GetDb(name).WithContext(ctx)
	}
	tx, ok := ctx.Value(_txKey).(*sql.Tx)
	if !ok {
		return GetDb(name).WithContext(ctx)
	}
	if ctx.Value(_nameKey) == nil {
		return GetDb(name).WithContext(ctx)
	}
	datasourceName, ok := ctx.Value(_nameKey).(string)
	if !ok {
		return GetDb(name).WithContext(ctx)
	}
	dbConfig, ok := _configMap[datasourceName]
	if !ok {
		return GetDb(name).WithContext(ctx)
	}
	dbType := dbConfig.Type
	var db *gorm.DB
	var err error
	if dbType == "" || strings.EqualFold(dbType, "mysql") {
		db, err = gorm.Open(mysql.New(mysql.Config{Conn: tx}), createConfigWithLog(dbConfig.LogLevel))
	} else if strings.EqualFold(dbType, "sqlserver") || strings.EqualFold(dbType, "mssql") {
		db, err = gorm.Open(sqlserver.New(sqlserver.Config{Conn: tx}), createConfigWithLog(dbConfig.LogLevel))
	} else if strings.EqualFold(dbType, "oracle") {
		db, err = gorm.Open(oracle.New(oracle.Config{Conn: tx}), createConfigWithLog(dbConfig.LogLevel))
	} else if strings.EqualFold(dbType, "postgres") || strings.EqualFold(dbType, "pgsql") {
		db, err = gorm.Open(postgres.New(postgres.Config{Conn: tx}), createConfigWithLog(dbConfig.LogLevel))
	}
	if err != nil {
		log.Println(err)
	}
	if db == nil {
		panic("get datasource db error")
	}
	return db.WithContext(ctx)
}

func InitDataSource(configMap map[string]Config) error {
	if len(configMap) == 0 {
		return errors.New("datasource config is empty")
	}
	_configMap = configMap
	for name, config := range configMap {
		db, err := createDb(config)
		if err != nil {
			return err
		}
		_dbMap[name] = db
	}
	return nil
}

func createDb(dbConfig Config) (*gorm.DB, error) {
	if dbConfig.Username == "" {
		return nil, ErrUsernameNotFound
	}
	var db *gorm.DB
	var err error
	dbType := strings.ReplaceAll(dbConfig.Type, " ", "")
	if dbType == "" || strings.EqualFold(dbType, "mysql") {
		dsn := dbConfig.Username + ":" + dbConfig.Password + "@tcp(" + dbConfig.Host + ")" + "/" + dbConfig.Dbname + "?charset=utf8mb4&parseTime=True&loc=Local"
		db, err = gorm.Open(mysql.Open(dsn), createConfigWithLog(dbConfig.LogLevel))
	} else if strings.EqualFold(dbType, "sqlserver") || strings.EqualFold(dbType, "mssql") {
		dsn := "sqlserver://" + dbConfig.Username + ":" + dbConfig.Password + "@" + dbConfig.Host + "?database=" + dbConfig.Dbname
		db, err = gorm.Open(sqlserver.Open(dsn), createConfigWithLog(dbConfig.LogLevel))
	} else if strings.EqualFold(dbType, "oracle") {
		dsn := `oracle://` + dbConfig.Username + `:` + dbConfig.Password + `@` + dbConfig.Host + `/` + dbConfig.Dbname
		db, err = gorm.Open(oracle.Open(dsn), createConfigWithLog(dbConfig.LogLevel))
	} else if strings.EqualFold(dbType, "oracle") {
		dsn := `oracle://` + dbConfig.Username + `:` + dbConfig.Password + `@` + dbConfig.Host + `/` + dbConfig.Dbname
		db, err = gorm.Open(oracle.Open(dsn), createConfigWithLog(dbConfig.LogLevel))
	} else if strings.EqualFold(dbType, "postgres") || strings.EqualFold(dbType, "pgsql") {
		host, port, err := net.SplitHostPort(dbConfig.Host)
		if err != nil {
			return nil, err
		}
		dsn := "host=" + host + " user=" + dbConfig.Username + " password=" + dbConfig.Password + " dbname=" + dbConfig.Dbname + " port=" + port + " sslmode=disable TimeZone=Asia/Shanghai"
		db, err = gorm.Open(oracle.Open(dsn), createConfigWithLog(dbConfig.LogLevel))
	}
	if err != nil {
		return nil, err
	}
	setDbProperties(db, dbConfig)
	return db, nil
}
