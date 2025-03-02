package scansion

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func buildResult(v any, fieldMap fieldMapType) error {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}

	if val.Kind() == reflect.Slice {
		sliceElem := reflect.New(val.Type().Elem()).Elem()
		if err := buildHelper(fieldMap, nil, sliceElem); err != nil {
			return err
		}
		if err := sliceMerge(fieldMap, val, sliceElem); err != nil {
			return err
		}
	} else {
		if err := buildHelper(fieldMap, nil, val); err != nil {
			return err
		}
	}

	return nil
}

func buildHelper(fieldMap fieldMapType, path []string, target reflect.Value) error {
	for _, childName := range getChildren(fieldMap, path) {
		newPath := append(path, childName)
		childPath := strings.Join(newPath, ".")
		childField := fieldMap.Map[childPath]

		if childField.Flat && childField.ScannedValue.IsValid() {
			target.FieldByIndex(childField.StructIdx).Set(childField.ScannedValue)
		}

		childType := childField.Type
		if childType.Kind() == reflect.Pointer {
			childType = childType.Elem()
		}

		var childTarget reflect.Value
		switch childType.Kind() {
		case reflect.Slice:
			childTarget = reflect.New(childField.Type.Elem()).Elem()
		case reflect.Struct:
			if childField.Flat {
				childTarget = childField.ScannedValue
			} else {
				localTarget := target
				if localTarget.Kind() == reflect.Pointer {
					localTarget = localTarget.Elem()
				}

				if localTarget.IsValid() {
					childTarget = localTarget.FieldByIndex(childField.StructIdx)
				}
			}
		case reflect.Pointer:
			fallthrough
		default:
			childTarget = childField.ScannedValue
		}

		if err := buildHelper(fieldMap, newPath, childTarget); err != nil {
			return err
		}

		if childTarget.IsValid() && !childTarget.IsZero() {
			localTarget := target
			if localTarget.Kind() == reflect.Pointer {
				if !localTarget.Elem().IsValid() {
					localTarget.Set(reflect.New(localTarget.Type().Elem()))
				}
				localTarget = localTarget.Elem()
			}

			if localTarget.Kind() == reflect.Struct {
				targetField := localTarget.FieldByIndex(childField.StructIdx)
				if targetField.Kind() == reflect.Slice {
					if err := sliceMerge(fieldMap, targetField, childTarget); err != nil {
						return err
					}
				} else {
					targetField.Set(childTarget)
				}
			} else {
				return fmt.Errorf("unexpected kind: %s", target.Kind())
			}
		}
	}

	return nil
}

func sliceMerge(fieldMap fieldMapType, slice, elem reflect.Value) error {
	if slice.Kind() != reflect.Slice {
		return errors.New("first argument must be a slice")
	}

	if slice.Type().Elem() != elem.Type() {
		return errors.New("both values must have the same primitve type")
	}

	startingSliceLen := slice.Len()
	for i := 0; i < startingSliceLen; i++ {
		sliceVal := slice.Index(i)
		slicePk, err := fieldMap.getPkValue(sliceVal)
		if err != nil {
			return err
		}
		elemPk, err := fieldMap.getPkValue(elem)
		if err != nil {
			return err
		}

		if !slicePk.Equal(elemPk) {
			// Only append if we're out of options
			if i == startingSliceLen-1 {
				slice.Set(reflect.Append(slice, elem))
			}
			continue
		}

		for fieldIdx := 0; fieldIdx < elem.NumField(); fieldIdx++ {
			sliceValField := sliceVal.Field(fieldIdx)
			elemField := elem.Field(fieldIdx)
			if elemField.Kind() == reflect.Slice && elemField.Type().Elem().Kind() == reflect.Struct {
				for elemIdx := 0; elemIdx < elemField.Len(); elemIdx++ {
					if err := sliceMerge(fieldMap, sliceValField, elemField.Index(elemIdx)); err != nil {
						return err
					}
				}
			} else if elemField.Kind() == reflect.Struct {
				if err := structMerge(fieldMap, sliceValField, elemField); err != nil {
					return err
				}
			}
		}
	}

	if slice.Len() == 0 {
		slice.Set(reflect.Append(slice, elem))
	}

	return nil
}

func structMerge(fieldMap fieldMapType, origStruct, newStruct reflect.Value) error {
	if origStruct.Kind() != reflect.Struct {
		return errors.New("first argument must be a struct")
	}

	if origStruct.Type() != newStruct.Type() {
		return errors.New("both values must have the same primitve type")
	}

	for fieldIdx := 0; fieldIdx < origStruct.NumField(); fieldIdx++ {
		sliceValField := origStruct.Field(fieldIdx)
		elemField := newStruct.Field(fieldIdx)
		if sliceValField.Kind() == reflect.Struct {
			if err := structMerge(fieldMap, sliceValField, elemField); err != nil {
				return err
			}
		} else if sliceValField.Kind() == reflect.Slice {
			for elemIdx := 0; elemIdx < elemField.Len(); elemIdx++ {
				if err := sliceMerge(fieldMap, sliceValField, elemField.Index(elemIdx)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
