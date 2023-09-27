package sqlscan

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
)

const dbTag = "db"

type fieldMapEntry struct {
	Type      reflect.Type
	Value     reflect.Value
	StructIdx int
}

type fieldMapType map[string]fieldMapEntry

func getFieldMap(s interface{}) (fieldMapType, error) {
	sVal := reflect.ValueOf(s)
	sType := reflect.TypeOf(s)
	if sType.Kind() != reflect.Pointer && sType.Elem().Kind() != reflect.Struct {
		return nil, errors.New("input is not a struct pointer")
	}

	rootMapEntry := fieldMapEntry{
		Type:  sType,
		Value: sVal,
	}

	fieldMap, err := getFieldMapHelper(s, nil, []reflect.Type{sType})
	if err != nil {
		return fieldMapType{}, err
	}

	fieldMap[""] = rootMapEntry

	return fieldMap, nil
}

func getFieldMapHelper(s interface{}, path []string, visited []reflect.Type) (fieldMapType, error) {
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
			visitedType := sValue.Field(i).Type()
			if visitedType.Kind() == reflect.Slice || visitedType.Kind() == reflect.Pointer {
				visitedType = visitedType.Elem()
			}
			if slices.Contains(path, dbTag) || slices.Contains(visited, visitedType) {
				continue
			}

			nestedMap, err := getFieldMapHelper(
				reflect.New(structFieldType.Type).Interface(),
				append(path, dbTag),
				append(visited, visitedType))
			if err != nil {
				return nil, err
			}
			maps.Copy(fieldMap, nestedMap)
		}

		scopedName := strings.Join(append(path, dbTag), ".")
		fieldMap[scopedName] = fieldMapEntry{
			Type:      structFieldType.Type,
			Value:     sValue.Field(i),
			StructIdx: i,
		}
	}

	return fieldMap, nil
}
