package sqlscan

import (
	"errors"
	"maps"
	"reflect"
	"strings"
)

const dbTag = "db"

type fieldMapEntry struct {
	Type        reflect.Type
	Value       reflect.Value
	ParentValue reflect.Value
	ParentType  reflect.Type
	Path        []string
	StructIdx   int
}

type fieldMapType map[string]fieldMapEntry

func getFieldMap(s interface{}) (fieldMapType, error) {
	sType := reflect.TypeOf(s)
	if sType.Kind() != reflect.Pointer && sType.Elem().Kind() != reflect.Struct {
		return nil, errors.New("input is not a struct pointer")
	}

	return getFieldMapHelper(s, nil, reflect.Value{})
}

func getFieldMapHelper(s interface{}, path []string, parentValue reflect.Value) (fieldMapType, error) {
	fieldMap := make(fieldMapType)

	sType := reflect.TypeOf(s).Elem()
	sValue := reflect.ValueOf(s).Elem()
	if sType.Kind() == reflect.Slice {
		sType = sType.Elem()
		sValue = reflect.New(sType).Elem()
	}

	for i := 0; i < sType.NumField(); i++ {
		structFieldType := sType.Field(i)
		dbTag := structFieldType.Tag.Get(dbTag)
		if dbTag == "" {
			continue
		}
		if strings.HasPrefix(dbTag, scanPrefix) {
			continue
		}

		if structFieldType.Type.Kind() == reflect.Slice {
			nestedMap, err := getFieldMapHelper(
				reflect.New(structFieldType.Type).Interface(),
				append(path, dbTag),
				sValue.Field(i))
			if err != nil {
				return nil, err
			}
			maps.Copy(fieldMap, nestedMap)
		}
		scopedName := strings.Join(append(path, dbTag), ".")

		mapEntry := fieldMapEntry{
			Type:        structFieldType.Type,
			Value:       sValue.Field(i),
			ParentValue: parentValue,
			Path:        path,
			StructIdx:   i,
		}
		if parentValue.IsValid() {
			mapEntry.ParentType = parentValue.Type()
		}

		fieldMap[scopedName] = mapEntry
	}

	return fieldMap, nil
}
