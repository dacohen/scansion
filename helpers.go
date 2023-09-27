package sqlscan

import (
	"reflect"
	"strings"
)

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

func getChildren(fieldMap fieldMapType, prefix []string) []string {
	var structChildren []string
	var primitiveChildren []string
	for k, v := range fieldMap {
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
