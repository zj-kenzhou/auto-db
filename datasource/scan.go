package datasource

import (
	"database/sql"
	"database/sql/driver"
	"github.com/zj-kenzhou/go-col/cmap"
	"log"
	"reflect"
)

func RowsToListMap(rows *sql.Rows) []cmap.Map[string, any] {
	columns, err := rows.Columns()
	if err != nil {
		log.Println(err)
	}
	columnTypes, _ := rows.ColumnTypes()
	var res []cmap.Map[string, any]
	for rows.Next() {
		node := cmap.NewLinkedHashMap[string, any]()
		values := make([]interface{}, len(columns))
		prepareValues(values, columnTypes, columns)
		rows.Scan(values...)
		scanIntoLinkedMap(node, values, columns)
		res = append(res, node)
	}
	return res
}

func prepareValues(values []interface{}, columnTypes []*sql.ColumnType, columns []string) {
	if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			value := createSlot(columnType)
			if value != nil {
				values[idx] = value
			} else if columnType.ScanType() != nil {
				values[idx] = reflect.New(reflect.PtrTo(columnType.ScanType())).Interface()
			} else {
				values[idx] = new(interface{})
			}
		}
	} else {
		for idx := range columns {
			values[idx] = new(interface{})
		}
	}
}

func scanIntoLinkedMap(mapValue cmap.Map[string, any], values []interface{}, columns []string) {
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			mapValue.Put(column, reflectValue.Interface())
			nodeValue := mapValue.Get(column)
			if b, ok := nodeValue.(sql.NullTime); ok {
				if b.Time.IsZero() {
					mapValue.Put(column, nil)
				} else {
					mapValue.Put(column, b.Time.Format("2006-01-02 15:04:05"))
				}
			} else if b, ok := nodeValue.(NullBool); ok {
				data, _ := b.BoolValue()
				mapValue.Put(column, data)
			} else if valuer, ok := nodeValue.(driver.Valuer); ok {
				data, _ := valuer.Value()
				mapValue.Put(column, data)
			} else if b, ok := nodeValue.(sql.RawBytes); ok {
				mapValue.Put(column, string(b))
			}
		} else {
			mapValue.Put(column, nil)
		}
	}
}

func createSlot(dbType *sql.ColumnType) interface{} {
	switch dbType.DatabaseTypeName() {
	case "INT", "TINYINT", "SMALLINT", "MEDIUMINT":
		return &sql.NullInt32{}
	case "NUMBER":
		precision, scale, _ := dbType.DecimalSize()
		if precision == 1 {
			return &NullBool{}
		}
		if precision > 9 {
			return &sql.NullString{}
		}
		if scale > 0 {
			return &sql.NullFloat64{}
		}
		return &sql.NullInt32{}
	case "FLOAT", "DOUBLE", "DECIMAL", "BINARY_DOUBLE", "BINARY_FLOAT":
		return &sql.NullFloat64{}
	case "CHAR":
		return &sql.NullByte{}
	case "VARCHAR", "TEXT", "LONGTEXT", "VARCHAR2", "NVARCHAR", "BIGINT":
		return &sql.NullString{}
	case "BIT", "BOOLEAN":
		return &NullBool{}
	case "DATE":
		return &sql.NullTime{}
	case "DATETIME":
		return &sql.NullTime{}
	case "TIMESTAMP":
		return &sql.NullTime{}
	}
	return nil
}
