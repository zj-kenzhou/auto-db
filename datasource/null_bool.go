package datasource

import (
	"database/sql/driver"
	"fmt"
	"github.com/spf13/cast"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"reflect"
	"strconv"
)

type BoolValue interface {
	BoolValue() (*bool, error)
}

// NullBool represents a bool that may be null.
// NullBool implements the Scanner interface
// it can be used as a scan destination, similar to NullString.
type NullBool struct {
	Val   int64
	Valid bool // Valid is true if Bool is not NULL
}

// Scan implements the Scanner interface.
func (n *NullBool) Scan(value any) error {
	if value == nil {
		n.Val, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	res, err := ConvertBoolValue(value)
	if res {
		n.Val = 1
	} else {
		n.Val = 0
	}
	return err
}
func ConvertBoolValue(src any) (bool, error) {
	switch s := src.(type) {
	case bool:
		return s, nil
	case string:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return false, fmt.Errorf("sql/driver: couldn't convert %q into type bool", s)
		}
		return b, nil
	case []byte:
		var data any
		data = s
		if len(s) == 1 {
			data = s[0]
		}
		b, err := strconv.ParseBool(cast.ToString(data))
		if err != nil {
			return false, fmt.Errorf("sql/driver: couldn't convert %q into type bool", s)
		}
		return b, nil
	}

	sv := reflect.ValueOf(src)
	switch sv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		iv := sv.Int()
		if iv == 1 || iv == 0 {
			return iv == 1, nil
		}
		return false, fmt.Errorf("sql/driver: couldn't convert %d into type bool", iv)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uv := sv.Uint()
		if uv == 1 || uv == 0 {
			return uv == 1, nil
		}
		return false, fmt.Errorf("sql/driver: couldn't convert %d into type bool", uv)
	}

	return false, fmt.Errorf("sql/driver: couldn't convert %v (%T) into type bool", src, src)
}

// Value implements the driver Valuer interface.
func (n NullBool) Value() (driver.Value, error) {
	if n.Valid {
		return n.Val, nil
	}
	return nil, nil
}

func (n NullBool) BoolValue() (*bool, error) {
	if n.Valid {
		res, err := ConvertBoolValue(n.Val)
		return &res, err
	}
	return nil, nil
}

// MarshalJSON on JSONTime format Time field with Y-m-d H:i:s
func (n NullBool) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	if n.Val > 0 {
		return []byte("true"), nil
	} else {
		return []byte("false"), nil
	}
}

func (n *NullBool) UnmarshalJSON(data []byte) (err error) {
	if data == nil || len(data) == 0 {
		n.Val, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	res, err := ConvertBoolValue(data)
	if res {
		n.Val = 1
	} else {
		n.Val = 0
	}
	return err
}

func (NullBool) GormDataType() string {
	return "BIT"
}

func (NullBool) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	switch db.Dialector.Name() {
	case "oracle":
		return "NUMBER(1)"
	}
	return "BIT"
}

func NewBool(value bool) NullBool {
	if value {
		return NullBool{
			Val:   1,
			Valid: true,
		}
	} else {
		return NullBool{
			Val:   0,
			Valid: true,
		}
	}
}
