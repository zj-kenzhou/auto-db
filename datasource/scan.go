package datasource

import (
	"database/sql"
	"database/sql/driver"
	"log"
	"reflect"
	"strings"
	"unicode"

	"github.com/zj-kenzhou/go-col/cmap"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func RowsToListMap(rows *sql.Rows) []cmap.Map[string, any] {
	columns, err := rows.Columns()
	if err != nil {
		log.Println(err)
	}
	columnTypes, _ := rows.ColumnTypes()
	res := make([]cmap.Map[string, any], 0)
	for rows.Next() {
		node := cmap.NewLinkedHashMap[string, any]()
		values := make([]interface{}, len(columns))
		prepareValues(values, columnTypes, columns)
		err := rows.Scan(values...)
		if err != nil {
			panic(err)
		}
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
				values[idx] = reflect.New(reflect.PointerTo(columnType.ScanType())).Interface()
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

func underLineToCamelCase(s string) string {
	s = strings.Replace(s, "_", " ", -1)
	s = cases.Title(language.Und).String(s)
	s = strings.Replace(s, " ", "", -1)
	return string(unicode.ToLower(rune(s[0]))) + s[1:]
}

func scanIntoLinkedMap(mapValue cmap.Map[string, any], values []interface{}, columns []string) {
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			mapValue.Put(underLineToCamelCase(column), reflectValue.Interface())
			nodeValue := mapValue.Get(underLineToCamelCase(column))
			if b, ok := nodeValue.(sql.NullTime); ok {
				if b.Time.IsZero() {
					mapValue.Put(underLineToCamelCase(column), nil)
				} else {
					mapValue.Put(underLineToCamelCase(column), b.Time.Format("2006-01-02 15:04:05"))
				}
			} else if b, ok := nodeValue.(NullBool); ok {
				data, _ := b.BoolValue()
				mapValue.Put(underLineToCamelCase(column), data)
			} else if valuer, ok := nodeValue.(driver.Valuer); ok {
				data, _ := valuer.Value()
				mapValue.Put(underLineToCamelCase(column), data)
			} else if b, ok := nodeValue.(sql.RawBytes); ok {
				mapValue.Put(underLineToCamelCase(column), string(b))
			}
		} else {
			mapValue.Put(underLineToCamelCase(column), nil)
		}
	}
}

func createSlot(dbType *sql.ColumnType) interface{} {
	switch dbType.DatabaseTypeName() {
	case "INT", "SMALLINT", "MEDIUMINT":
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
	case "VARCHAR", "TEXT", "LONGTEXT", "VARCHAR2", "NVARCHAR", "BIGINT", "UNSIGNED BIGINT":
		return &sql.NullString{}
	case "TINYINT", "BIT", "BOOLEAN":
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
