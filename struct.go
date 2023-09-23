package sqlscan

import (
	"errors"
	"reflect"
)

const dbTag = "db"

type fieldMapEntry struct {
	Type  reflect.Type
	Value reflect.Value
}

type fieldMapType map[string]fieldMapEntry

func getFieldMap(s interface{}) (fieldMapType, error) {
	fieldMap := make(fieldMapType)

	sType := reflect.TypeOf(s)
	if sType.Kind() != reflect.Pointer && sType.Elem().Kind() != reflect.Struct {
		return nil, errors.New("input is not a struct pointer")
	}
	sType = sType.Elem()
	sValue := reflect.ValueOf(s).Elem()

	for i := 0; i < sType.NumField(); i++ {
		structFieldType := sType.Field(i)
		dbTag := structFieldType.Tag.Get(dbTag)
		if dbTag == "" {
			continue
		}

		fieldMap[dbTag] = fieldMapEntry{
			Type:  structFieldType.Type,
			Value: sValue.Field(i),
		}
	}

	return fieldMap, nil
}
