package sqlscan

import (
	"fmt"
	"reflect"

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

		if p.rows.Next() {
			return p.scanRow(p.rows.Scan, fieldMap)
		}
	case reflect.Slice:
		sliceOf := getSliceElementType(vType)
		for p.rows.Next() {
			sliceVal := reflect.New(sliceOf)
			fieldMap, err := getFieldMap(sliceVal.Interface())
			if err != nil {
				return err
			}
			if err = p.scanRow(p.rows.Scan, fieldMap); err != nil {
				return err
			}
			vValue.Set(reflect.Append(vValue, sliceVal.Elem()))
		}
	}

	return nil
}

func (p *PgxScanner) scanRow(scan scannerFunc, fieldMap fieldMapType) error {
	target := make([]interface{}, len(p.rows.FieldDescriptions()))
	fields := make([]fieldMapEntry, len(p.rows.FieldDescriptions()))

	for i, desc := range p.rows.FieldDescriptions() {
		fieldEntry, ok := fieldMap[desc.Name]
		if !ok {
			return fmt.Errorf("field %s not defined in scan target", desc.Name)
		}
		target[i] = reflect.New(fieldEntry.Type).Interface()
		fields[i] = fieldEntry
	}

	if err := p.rows.Scan(target...); err != nil {
		return err
	}

	for idx, t := range target {
		fields[idx].Value.Set(reflect.ValueOf(t).Elem())
	}

	return nil
}
