package scansion

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type SqlScanner struct {
	rows *sql.Rows
}

func NewSqlScanner(rows *sql.Rows) *SqlScanner {
	return &SqlScanner{
		rows: rows,
	}
}

func (s *SqlScanner) Scan(v interface{}) error {
	defer s.rows.Close()

	for s.rows.Next() {
		fieldMap, err := getFieldMap(v)
		if err != nil {
			return err
		}

		if err = s.scanRow(s.rows.Scan, fieldMap); err != nil {
			return err
		}

		buildHelper(fieldMap, nil)
	}

	return nil
}

func (s *SqlScanner) scanRow(scan scannerFunc, fieldMap fieldMapType) error {
	columnTypes, err := s.rows.ColumnTypes()
	if err != nil {
		return err
	}
	targets := make([]interface{}, len(columnTypes))
	fields := make([]fieldMapEntry, len(columnTypes))

	var path []string
	var scanColIdxs []int
	for i, col := range columnTypes {
		if strings.HasPrefix(col.Name(), scanPrefix) {
			scanField := strings.TrimPrefix(col.Name(), scanPrefix)
			path = strings.Split(scanField, ".")

			// database/sql cannot scan into nil, so we create a placeholder
			targets[i] = reflect.New(reflect.PointerTo(reflect.TypeOf(0))).Interface()
			scanColIdxs = append(scanColIdxs, i)

			continue
		}

		scopedName := strings.Join(append(path, col.Name()), ".")
		fieldEntry, ok := fieldMap[scopedName]
		if !ok {
			return fmt.Errorf("field %s not defined in scan target", scopedName)
		}

		targetType := fieldEntry.Type
		if fieldEntry.Optional {
			targetType = reflect.PointerTo(targetType)
		}

		targets[i] = reflect.New(targetType).Interface()
		fields[i] = fieldEntry
	}

	if err := s.rows.Scan(targets...); err != nil {
		return err
	}

	// We want to preserve the nil boundaries between tables, so we unset them after scan
	for _, scanColIdx := range scanColIdxs {
		targets[scanColIdx] = nil
	}

	for idx, t := range targets {
		if t == nil {
			// Scan column
			continue
		}

		targetVal := reflect.ValueOf(t).Elem()
		currentField := fields[idx]
		if currentField.Optional {
			if targetVal.IsNil() {
				continue
			}
			targetVal = targetVal.Elem()
		}

		if currentField.Value.IsZero() {
			currentField.Value.Set(targetVal)
		}
	}

	return nil
}
