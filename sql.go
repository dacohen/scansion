package scansion

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// PgxScanner wraps the *sql.Rows result set in Rows
type SqlScanner struct {
	Rows *sql.Rows
}

// NewPgxScanner takes a *sql.Rows struct and returns a SqlScanner
func NewSqlScanner(rows *sql.Rows) *SqlScanner {
	return &SqlScanner{
		Rows: rows,
	}
}

// Scan maps the wrapped Rows into the provided interface.
// Unless exactly one result is expected (e.g. LIMIT 1 is used)
// a slice is the expected argument.
func (s *SqlScanner) Scan(v any) (err error) {
	var rowCount int

	defer func() {
		s.Rows.Close()
		if err == nil && rowCount == 0 {
			err = sql.ErrNoRows
		}
	}()

	for s.Rows.Next() {
		fieldMap, err := getFieldMap(v)
		if err != nil {
			return err
		}

		if err = s.scanRow(fieldMap); err != nil {
			return err
		}

		if err = buildResult(v, fieldMap); err != nil {
			return err
		}
		rowCount++
	}

	return nil
}

func (s *SqlScanner) scanRow(fieldMap fieldMapType) error {
	columnTypes, err := s.Rows.ColumnTypes()
	if err != nil {
		return err
	}
	targets := make([]any, len(columnTypes))
	fields := make([]fieldMapEntry, len(columnTypes))
	scopedNames := make([]string, len(columnTypes))

	var path []string
	var scanColIdxs []int
	for i, col := range columnTypes {
		if strings.HasPrefix(col.Name(), scanPrefix) {
			scanField := strings.TrimPrefix(col.Name(), scanPrefix)
			path = strings.Split(scanField, ".")

			// database/sql cannot scan into nil, so we create a placeholder for the zero columns
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
		if fieldEntry.Optional && targetType.Kind() != reflect.Pointer {
			targetType = reflect.PointerTo(targetType)
		}

		targets[i] = reflect.New(targetType).Interface()
		fields[i] = fieldEntry
		scopedNames[i] = scopedName
	}

	if err := s.Rows.Scan(targets...); err != nil {
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
		if currentField.Optional && targetVal.Kind() == reflect.Pointer &&
			currentField.Type.Kind() != reflect.Pointer {
			if targetVal.IsNil() {
				continue
			}
			targetVal = targetVal.Elem()
		}

		currentField.ScannedValue = targetVal
		fieldMap[scopedNames[idx]] = currentField
	}

	return nil
}
