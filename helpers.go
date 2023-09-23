package sqlscan

import "reflect"

func getSliceElementType(typ reflect.Type) reflect.Type {
	sliceType := typ.Elem()
	if sliceType.Kind() == reflect.Pointer {
		sliceType = sliceType.Elem()
	}

	return sliceType
}
