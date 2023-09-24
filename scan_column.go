package sqlscan

import (
	"errors"
	"regexp"
	"strings"
)

type scanMode string

const (
	scanModeOne  scanMode = "one"
	scanModeMany scanMode = "many"

	scanPrefix = "scan:"
)

type scanColumn struct {
	Mode       scanMode
	FieldName  string
	GroupByKey string
}

func mapTrim(s []string) []string {
	for i := range s {
		s[i] = strings.TrimSpace(s[i])
	}
	return s
}

func parseScanColumn(colName string) (scanColumn, error) {
	body := strings.TrimPrefix(colName, scanPrefix)

	oneRegex := regexp.MustCompile(`one\((.*)\)`)
	manyRegex := regexp.MustCompile(`many\((.*)\)`)
	oneMatches := oneRegex.FindStringSubmatch(body)
	if len(oneMatches) == 2 {
		panic("Not implemented")
	}

	manyMatches := manyRegex.FindStringSubmatch(body)
	if len(manyMatches) == 2 {
		parts := mapTrim(strings.Split(manyMatches[1], ","))
		if len(parts) == 2 {
			return scanColumn{
				Mode:       scanModeMany,
				FieldName:  parts[0],
				GroupByKey: parts[1],
			}, nil
		}
	}

	return scanColumn{}, errors.New("unable to parse scan column")
}
