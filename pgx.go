package sqlscan

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
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

	vType := reflect.TypeOf(v).Elem()
	vValue := reflect.ValueOf(v).Elem()

	switch vType.Kind() {
	case reflect.Struct:
		fieldMap, err := getFieldMap(v)
		if err != nil {
			return err
		}

		for p.rows.Next() {
			if err = p.scanRow(p.rows.Scan, fieldMap, false); err != nil {
				return err
			}
		}
	case reflect.Slice:
		sliceOf := getSliceElementType(vType)

		currentGroupByValMap := make(map[string]reflect.Value)
		for _, desc := range p.rows.FieldDescriptions() {
			if strings.HasPrefix(desc.Name, scanPrefix) {
				scanCol, err := parseScanColumn(desc.Name)
				if err != nil {
					return err
				}
				currentGroupByValMap[scanCol.GroupByKey] = reflect.Value{}
			}
		}
		sliceVal := reflect.New(sliceOf)
		for p.rows.Next() {
			tempFieldMap, err := getFieldMap(reflect.New(sliceOf).Interface())
			if err != nil {
				return err
			}

			if err = p.scanRow(p.rows.Scan, tempFieldMap, true); err != nil {
				return err
			}

			for k, v := range currentGroupByValMap {
				fieldVal := tempFieldMap[k].Value
				if v.IsValid() && !v.Equal(fieldVal) {
					vValue.Set(reflect.Append(vValue, sliceVal.Elem()))
					sliceVal = reflect.New(sliceOf)
				}
				currentGroupByValMap[k] = tempFieldMap[k].Value
			}

			fieldMap, err := getFieldMap(sliceVal.Interface())
			if err != nil {
				return err
			}

			if err = p.scanRow(p.rows.Scan, fieldMap, false); err != nil {
				return err
			}
		}

		if !sliceVal.Elem().IsZero() {
			vValue.Set(reflect.Append(vValue, sliceVal.Elem()))
		}
	}

	return nil
}

func (p *PgxScanner) scanRow(scan scannerFunc, fieldMap fieldMapType, speculative bool) error {
	target := make([]interface{}, len(p.rows.FieldDescriptions()))
	fields := make([]fieldMapEntry, len(p.rows.FieldDescriptions()))

	var path []string
	for i, desc := range p.rows.FieldDescriptions() {
		if strings.HasPrefix(desc.Name, scanPrefix) {
			scanCol, err := parseScanColumn(desc.Name)
			if err != nil {
				return err
			}
			path = strings.Split(scanCol.FieldName, ".")
			continue
		}

		scopedName := strings.Join(append(path, desc.Name), ".")
		fieldEntry, ok := fieldMap[scopedName]
		if !ok {
			return fmt.Errorf("field %s not defined in scan target", scopedName)
		}
		target[i] = reflect.New(fieldEntry.Type).Interface()
		fields[i] = fieldEntry
	}

	if err := p.rows.Scan(target...); err != nil {
		return err
	}

	var nestedTarget reflect.Value
	var nestedPath []string
	for idx, t := range target {
		lastTarget := idx == len(target)-1
		if t == nil {
			// Scan column
			continue
		}
		targetVal := reflect.ValueOf(t).Elem()
		currentField := fields[idx]
		if currentField.ParentType != nil {
			if currentField.ParentType.Kind() == reflect.Slice {
				if !nestedTarget.IsValid() {
					sliceOf := getSliceElementType(currentField.ParentType)
					nestedTarget = reflect.New(sliceOf).Elem()
					nestedPath = currentField.Path
				}
				nestedTarget.Field(currentField.StructIdx).Set(targetVal)
				if !slices.Equal(currentField.Path, nestedPath) || lastTarget {
					currentField.ParentValue.Set(reflect.Append(currentField.ParentValue, nestedTarget))
					nestedTarget = reflect.Value{}
					nestedPath = nil
				}
			}
		} else {
			if !speculative {
				currentCompare := currentField.Value
				if currentCompare.Kind() == reflect.Pointer {
					currentCompare = currentCompare.Elem()
				}
				targetCompare := targetVal
				if targetCompare.Kind() == reflect.Pointer {
					targetCompare = targetCompare.Elem()
				}
				if !currentField.Value.IsZero() && !currentCompare.Equal(targetCompare) {
					return errors.New("parent value incorrectly overwritten")
				}
			}
			currentField.Value.Set(targetVal)
		}
	}

	return nil
}
