package sqlscan

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
)

type PgxScanner struct {
	rows pgx.Rows
}

func NewPgxScanner(rows pgx.Rows) *PgxScanner {
	return &PgxScanner{
		rows: rows,
	}
}

func (p *PgxScanner) Scan(v interface{}) error {
	defer p.rows.Close()

	for p.rows.Next() {
		fieldMap, err := getFieldMap(v)
		if err != nil {
			return err
		}

		if err = p.scanRow(p.rows.Scan, fieldMap); err != nil {
			return err
		}

		buildHelper(fieldMap, nil)
	}

	return nil
}

func (p *PgxScanner) scanRow(scan scannerFunc, fieldMap fieldMapType) error {
	targets := make([]interface{}, len(p.rows.FieldDescriptions()))
	fields := make([]fieldMapEntry, len(p.rows.FieldDescriptions()))

	var path []string
	for i, desc := range p.rows.FieldDescriptions() {
		if strings.HasPrefix(desc.Name, scanPrefix) {
			scanField := strings.TrimPrefix(desc.Name, scanPrefix)
			path = strings.Split(scanField, ".")
			continue
		}

		scopedName := strings.Join(append(path, desc.Name), ".")
		fieldEntry, ok := fieldMap[scopedName]
		if !ok {
			return fmt.Errorf("field %s not defined in scan target", scopedName)
		}
		targets[i] = reflect.New(fieldEntry.Type).Interface()
		fields[i] = fieldEntry
	}

	if err := p.rows.Scan(targets...); err != nil {
		return err
	}

	for idx, t := range targets {
		if t == nil {
			// Scan column
			continue
		}

		targetVal := reflect.ValueOf(t).Elem()
		currentField := fields[idx]

		if currentField.Value.IsZero() {
			currentField.Value.Set(targetVal)
		}
	}

	return nil
}

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

		buildHelper(fieldMap, newPath)

		if childField.Value.IsValid() {
			targetVal.Field(childField.StructIdx).Set(childField.Value)
		}
	}

	if len(children) == 0 {
		targetVal = fieldMap[parentName].Value
	}

	if targetVal.IsZero() {
		return
	}

	if sliceTargetVal.IsValid() && targetVal.IsValid() {
		// Here's where we do a recursive merge
		sliceMerge(sliceTargetVal, targetVal)
	}

	parentVal := parentField.Value
	if parentVal.Kind() == reflect.Slice {
		parentVal.Set(reflect.Append(parentVal, targetVal))
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
