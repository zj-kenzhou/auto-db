package datasource

import (
	"encoding/json"
	"testing"
)

func TestGetData(t *testing.T) {
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
	db := Db()
	rows, err := db.Table("SYS_MENU").Rows()
	if err != nil {
		t.Error(err)
	}
	listMap := RowsToListMap(rows)
	res, _ := json.Marshal(listMap)
	t.Log(string(res))
}
