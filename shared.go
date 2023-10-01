package scansion

import (
	"errors"
	"reflect"
	"strings"
)

func buildHelper(fieldMap fieldMapType, path []string) {
	parentName := strings.Join(path, ".")
	parentField := fieldMap[parentName]

	children := getChildren(fieldMap, path)

	var sliceTargetVal reflect.Value
	var targetVal reflect.Value
	if parentField.Value.Kind() == reflect.Slice {
		targetVal = reflect.New(parentField.Type.Elem()).Elem()
	} else if parentField.Value.Kind() == reflect.Pointer {
		targetVal = parentField.Value.Elem()
	} else if !parentField.Value.IsValid() {
		targetVal = reflect.New(parentField.Type).Elem()
	} else {
		targetVal = parentField.Value
	}

	if targetVal.Kind() == reflect.Slice {
		// We'll be working on a single element, so save a reference to the slice for later
		sliceTargetVal = targetVal
		targetVal = reflect.New(targetVal.Type().Elem()).Elem()
	}

	for _, childName := range children {
		newPath := append(path, childName)
		childField := fieldMap[strings.Join(newPath, ".")]

		if childField.Value.Kind() == reflect.Pointer && childField.Value.IsNil() {
			childField.Value.Set(reflect.New(childField.Type.Elem()))
		}

		buildHelper(fieldMap, newPath)

		if childField.Value.IsValid() {
			targetVal.FieldByIndex(childField.StructIdx).Set(childField.Value)
		}
	}

	if len(children) == 0 {
		targetVal = fieldMap[parentName].Value
	}

	parentVal := parentField.Value
	if !targetVal.IsValid() || targetVal.IsZero() ||
		(targetVal.Kind() == reflect.Pointer && targetVal.Elem().IsZero()) {
		// If the target is nil, clean up the empty parent element as well
		parentVal.Set(reflect.Zero(parentVal.Type()))
		return
	}

	if sliceTargetVal.IsValid() && targetVal.IsValid() {
		// Recursively merge the slices
		sliceMerge(sliceTargetVal, targetVal)
	}

	if parentVal.Kind() == reflect.Slice {
		if parentField.Flat {
			parentVal.Set(targetVal)
		} else {
			parentVal.Set(reflect.Append(parentVal, targetVal))
		}
	} else if parentVal.Kind() == reflect.Pointer &&
		parentVal.Elem().Kind() != reflect.Slice {
		if targetVal.Kind() == reflect.Pointer {
			targetVal = targetVal.Elem()
		}
		parentVal.Elem().Set(targetVal)
	}
}

func sliceMerge(slice, elem reflect.Value) error {
	if slice.Kind() != reflect.Slice {
		return errors.New("first argument must be a slice")
	}

	if slice.Type().Elem() != elem.Type() {
		return errors.New("both values must have the same primitve type")
	}

	startingSliceLen := slice.Len()
	for i := 0; i < startingSliceLen; i++ {
		sliceVal := slice.Index(i)
		slicePk, err := getPkValue(sliceVal)
		if err != nil {
			return err
		}
		elemPk, err := getPkValue(elem)
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
			if elemField.Kind() == reflect.Slice {
				for elemIdx := 0; elemIdx < elemField.Len(); elemIdx++ {
					if err := sliceMerge(sliceValField, elemField.Index(elemIdx)); err != nil {
						return err
					}
				}
			}
		}
	}

	if slice.Len() == 0 {
		slice.Set(reflect.Append(slice, elem))
	}

	return nil
}
