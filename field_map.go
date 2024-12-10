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
	Type      reflect.Type
	Value     reflect.Value
	StructIdx []int
	Optional  bool
	Flat      bool
}

type fieldMapType map[string]fieldMapEntry

func getFieldMap(s any) (fieldMapType, error) {
	sVal := reflect.ValueOf(s)
	sType := reflect.TypeOf(s)
	if sType.Kind() != reflect.Pointer && sType.Elem().Kind() != reflect.Struct {
		return nil, errors.New("input is not a struct pointer")
	}

	rootMapEntry := fieldMapEntry{
		Type:  sType,
		Value: sVal,
	}

	fieldMap, err := getFieldMapHelper(s, nil, nil, []reflect.Type{sType}, false)
	if err != nil {
		return fieldMapType{}, err
	}

	fieldMap[""] = rootMapEntry

	return fieldMap, nil
}

func getFieldMapHelper(s any, path []string, idxPath []int, visited []reflect.Type, optional bool) (fieldMapType, error) {
	fieldMap := make(fieldMapType)

	sType := reflect.TypeOf(s).Elem()
	sValue := reflect.ValueOf(s).Elem()
	if sType.Kind() == reflect.Slice {
		sType = sType.Elem()
		sValue = reflect.New(sType).Elem()
	}

	for i := 0; i < sType.NumField(); i++ {
		structField := sType.Field(i)
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
		isFlat := slices.Contains(extraOptions, dbTagOptionFlat)
		canRecurse := !scannable && !isFlat && !isBuiltinStruct(structField.Type)

		if canRecurse {
			if structField.Type.Kind() == reflect.Slice {
				visitedType := structField.Type
				if visitedType.Kind() == reflect.Slice || visitedType.Kind() == reflect.Pointer {
					visitedType = visitedType.Elem()
				}
				if slices.Contains(path, dbFieldName) || slices.Contains(visited, visitedType) {
					continue
				}

				nestedMap, err := getFieldMapHelper(
					reflect.New(visitedType).Interface(),
					append(path, dbFieldName),
					idxPath,
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
					reflect.New(visitedType).Interface(),
					append(path, dbFieldName),
					idxPath,
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
					reflect.New(visitedType).Interface(),
					path,
					append(idxPath, i),
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
					reflect.New(visitedType).Interface(),
					append(path, dbFieldName),
					idxPath,
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
			Value:     sValue.Field(i),
			StructIdx: append(idxPath, i),
			Optional:  optional,
			Flat:      isFlat,
		}
	}

	return fieldMap, nil
}

func getPkValue(s reflect.Value) (reflect.Value, error) {
	var pkValue reflect.Value

	if s.Kind() == reflect.Pointer {
		s = s.Elem()
	}

	if s.Kind() != reflect.Struct {
		return reflect.Value{}, errors.New("input must be of type struct")
	}

	for i := 0; i < s.NumField(); i++ {
		fieldType := s.Type().Field(i)
		fieldVal := s.Field(i)
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
