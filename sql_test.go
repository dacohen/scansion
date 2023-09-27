package scansion_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/dacohen/scansion"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
)

func setupSqlDb(t *testing.T, queries []string, tx *sql.Tx) {
	t.Helper()

	for _, query := range queries {
		_, err := tx.Exec(query)
		assert.NoError(t, err)
	}
}

func TestSqlScan(t *testing.T) {
	dbUrl, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
		dbUrl = "host=localhost user=postgres dbname=scansion_test"
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, err := sql.Open("pgx", dbUrl)
			assert.NoError(t, err)
			defer db.Close()

			tx, err := db.Begin()
			assert.NoError(t, err)
			defer tx.Rollback()

			setupSqlDb(t, setupQueries, tx)

			rows, err := tx.Query(testCase.query)
			assert.NoError(t, err)

			scanner := scansion.NewSqlScanner(rows)
			if testCase.manyRows {
				var target []Author
				err = scanner.Scan(&target)
				assert.NoError(t, err)
				assert.EqualValues(t, testCase.expected, target)
			} else {
				var target Author
				err = scanner.Scan(&target)
				assert.NoError(t, err)
				assert.EqualValues(t, testCase.expected, target)
			}
		})
	}
}
