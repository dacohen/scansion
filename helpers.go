package scansion

import (
	"fmt"
	"reflect"
	"strings"
)

func mapFn[T any, U any](s []T, fn func(T) U) []U {
	result := make([]U, len(s))
	for i, v := range s {
		result[i] = fn(v)
	}
	return result
}

func hasPrefix[T comparable](s []T, p []T) bool {
	if len(p) == 0 {
		return true
	}

	if len(s) < len(p) {
		return false
	}

	for i := range p {
		if s[i] != p[i] {
			return false
		}
	}
	return true
}

func getChildren(fm fieldMap, prefix []string) []string {
	var structChildren []string
	var primitiveChildren []string
	for k, v := range fm.Map {
		if k == "" {
			continue
		}
		vType := v.Type
		if vType.Kind() == reflect.Pointer || vType.Kind() == reflect.Slice {
			vType = vType.Elem()
		}

		keyParts := strings.Split(k, ".")
		lastKeyPart := keyParts[len(keyParts)-1]
		if hasPrefix(keyParts, prefix) && len(keyParts) == len(prefix)+1 {
			if vType.Kind() == reflect.Struct {
				structChildren = append(structChildren, lastKeyPart)
			} else {
				primitiveChildren = append(primitiveChildren, lastKeyPart)
			}
		}
	}

	return append(primitiveChildren, structChildren...)
}

func isBuiltinStruct(typ reflect.Type) bool {
	if typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}

	fullName := typ.Name()
	if typ.PkgPath() != "" {
		fullName = fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name())
	}

	switch fullName {
	case "time.Time":
		return true
	default:
		return false
	}
}
