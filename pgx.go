package scansion

import (
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

		targetType := fieldEntry.Type
		if fieldEntry.Optional {
			targetType = reflect.PointerTo(targetType)
		}

		targets[i] = reflect.New(targetType).Interface()
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
