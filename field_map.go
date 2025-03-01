package scansion

import (
	"database/sql"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
)

const (
	dbTagName       = "db"
	dbTagIgnore     = "-"
	dbTagOptionPk   = "pk"
	dbTagOptionFlat = "flat"

	scanPrefix = "scan:"
)

type fieldMapEntry struct {
	Type         reflect.Type
	ScannedValue reflect.Value
	StructIdx    []int
	Optional     bool
	Flat         bool
}

type fieldMapType map[string]fieldMapEntry

func getFieldMap(v any) (fieldMapType, error) {
	vType := reflect.TypeOf(v)
	if vType.Kind() != reflect.Pointer || (vType.Elem().Kind() != reflect.Struct && vType.Elem().Kind() != reflect.Slice) {
		return nil, errors.New("input is not a struct or slice pointer")
	}

	rootMapEntry := fieldMapEntry{
		Type: vType,
	}

	fieldMap, err := getFieldMapHelper(vType.Elem(), nil, nil, []reflect.Type{vType}, false)
	if err != nil {
		return fieldMapType{}, err
	}

	fieldMap[""] = rootMapEntry

	return fieldMap, nil
}

func getFieldMapHelper(vType reflect.Type, path []string, idxPath []int, visited []reflect.Type, optional bool) (fieldMapType, error) {
	fieldMap := make(fieldMapType)

	if vType.Kind() == reflect.Slice {
		vType = vType.Elem()
	}

	for i := 0; i < vType.NumField(); i++ {
		structField := vType.Field(i)
		fullDbTag := structField.Tag.Get(dbTagName)
		if (fullDbTag == "" && !structField.Anonymous) || fullDbTag == dbTagIgnore {
			continue
		}
		dbTagParts := strings.Split(fullDbTag, ",")
		dbTagParts = mapFn(dbTagParts, strings.TrimSpace)

		dbFieldName := fullDbTag
		var extraOptions []string
		if len(dbTagParts) > 1 {
			dbFieldName = dbTagParts[0]
			extraOptions = dbTagParts[1:]
		}

		scannable := structField.Type.Implements(reflect.TypeOf(new(sql.Scanner)).Elem())
		isFlat := slices.Contains(extraOptions, dbTagOptionFlat) ||
			(structField.Type.Kind() == reflect.Slice && structField.Type.Elem().Kind() != reflect.Struct)
		canRecurse := !scannable && !isFlat && !isBuiltinStruct(structField.Type)

		if canRecurse {
			if structField.Type.Kind() == reflect.Slice {
				visitedType := structField.Type.Elem()
				if slices.Contains(path, dbFieldName) || slices.Contains(visited, visitedType) {
					continue
				}

				nestedMap, err := getFieldMapHelper(
					visitedType,
					append(path, dbFieldName),
					nil,
					append(visited, visitedType),
					true)
				if err != nil {
					return nil, err
				}
				maps.Copy(fieldMap, nestedMap)
			} else if structField.Type.Kind() == reflect.Pointer &&
				structField.Type.Elem().Kind() == reflect.Struct {
				visitedType := structField.Type.Elem()

				if slices.Contains(path, dbFieldName) || slices.Contains(visited, visitedType) {
					continue
				}

				nestedMap, err := getFieldMapHelper(
					visitedType,
					append(path, dbFieldName),
					nil,
					append(visited, visitedType),
					true)
				if err != nil {
					return nil, err
				}
				maps.Copy(fieldMap, nestedMap)
			} else if structField.Type.Kind() == reflect.Struct && structField.Anonymous {
				// Embedded struct
				visitedType := structField.Type

				nestedMap, err := getFieldMapHelper(
					visitedType,
					path,
					[]int{i},
					visited,
					false)
				if err != nil {
					return nil, err
				}
				maps.Copy(fieldMap, nestedMap)

				// Since this is an embedded struct, we should NOT create an entry in the fieldMap for it
				continue
			} else if structField.Type.Kind() == reflect.Struct {
				visitedType := structField.Type

				if slices.Contains(path, dbFieldName) || slices.Contains(visited, visitedType) {
					continue
				}

				nestedMap, err := getFieldMapHelper(
					visitedType,
					append(path, dbFieldName),
					nil,
					append(visited, visitedType),
					false)
				if err != nil {
					return nil, err
				}
				maps.Copy(fieldMap, nestedMap)
			}
		}

		scopedName := strings.Join(append(path, dbFieldName), ".")
		fieldMap[scopedName] = fieldMapEntry{
			Type:      structField.Type,
			StructIdx: append(idxPath, i),
			Optional:  optional,
			Flat:      !canRecurse,
		}
	}

	return fieldMap, nil
}

func getPkValue(v reflect.Value) (reflect.Value, error) {
	var pkValue reflect.Value

	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return reflect.Value{}, errors.New("input must be of type struct")
	}

	for i := 0; i < v.NumField(); i++ {
		fieldType := v.Type().Field(i)
		fieldVal := v.Field(i)
		fullDbTag := fieldType.Tag.Get(dbTagName)
		if fullDbTag == "" {
			continue
		}
		dbTagParts := strings.Split(fullDbTag, ",")
		dbTagParts = mapFn(dbTagParts, strings.TrimSpace)

		if len(dbTagParts) == 2 && dbTagParts[1] == dbTagOptionPk {
			if pkValue.IsValid() {
				return reflect.Value{}, errors.New("exactly one column must have 'pk' set")
			}
			pkValue = fieldVal
		}
	}

	if !pkValue.IsValid() {
		return reflect.Value{}, errors.New("exactly one column must have 'pk' set")
	}

	return pkValue, nil
}
