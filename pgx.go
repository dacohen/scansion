package scansion

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
)

// PgxScanner wraps the pgx.Rows result set in Rows
type PgxScanner struct {
	Rows pgx.Rows
}

// NewPgxScanner takes a pgx.Rows struct and returns a PgxScanner
func NewPgxScanner(rows pgx.Rows) *PgxScanner {
	return &PgxScanner{
		Rows: rows,
	}
}

// Scan maps the wrapped Rows into the provided interface.
// Unless exactly one result is expected (e.g. LIMIT 1 is used)
// a slice is the expected argument.
func (p *PgxScanner) Scan(v any) (err error) {
	var rowCount int

	defer func() {
		p.Rows.Close()
		if err == nil && rowCount == 0 {
			err = pgx.ErrNoRows
		}
	}()

	for p.Rows.Next() {
		fieldMap, err := getFieldMap(v)
		if err != nil {
			return err
		}

		if err = p.scanRow(fieldMap); err != nil {
			return err
		}

		if err = buildHelper(fieldMap, nil); err != nil {
			return err
		}
		rowCount++
	}

	return nil
}

func (p *PgxScanner) scanRow(fieldMap fieldMapType) error {
	fieldDescriptions := p.Rows.FieldDescriptions()
	targets := make([]any, len(fieldDescriptions))
	fields := make([]fieldMapEntry, len(fieldDescriptions))

	var path []string
	for i, desc := range fieldDescriptions {
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

		targetType := fieldEntry.Type
		if fieldEntry.Optional && targetType.Kind() != reflect.Pointer {
			targetType = reflect.PointerTo(targetType)
		}

		targets[i] = reflect.New(targetType).Interface()
		fields[i] = fieldEntry
	}

	if err := p.Rows.Scan(targets...); err != nil {
		return err
	}

	for idx, t := range targets {
		if t == nil {
			// Scan column
			continue
		}

		targetVal := reflect.ValueOf(t).Elem()
		currentField := fields[idx]
		if currentField.Optional && targetVal.Kind() == reflect.Pointer &&
			currentField.Value.Kind() != reflect.Pointer {
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
