package datasource

import (
	"context"
	"testing"
)

func TestTransaction(t *testing.T) {
	err := InitDataSource(map[string]Config{
		"primary": {
			Type:            "mysql",
			Host:            "127.0.0.1:3306",
			Username:        "root",
			Password:        "root",
			LogLevel:        4,
			Dbname:          "test",
			MaxIdleConns:    0,
			MaxOpenConns:    2,
			ConnMaxLifetime: 2,
			ConnMaxIdleTime: 1,
			AutoMigrate:     false,
		},
	})
	if err != nil {
		t.Error(err)
	}
	rootCtx := context.Background()
	err = Transaction(rootCtx, func(txCtx context.Context) error {
		db := GetDbByCtx(txCtx)
		err := db.Exec("INSERT FK_SYS_ROLE_LINK_SYS_MENU VALUES(1,2)").Error
		if err != nil {
			return err
		}
		err = db.Exec("INSERT FK_SYS_ROLE_LINK_SYS_MENU VALUES(2,3)").Error
		if err != nil {
			return err
		}
		return db.Exec("INSERT FK_SYS_ROLE_LINK_SYS_MENU VALUES(4,5)").Error
	})
	if err != nil {
		t.Error(err)
	}
}
