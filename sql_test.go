package scansion_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/dacohen/scansion"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupSqlDb(t *testing.T, queries []string, tx *sql.Tx) {
	t.Helper()

	for _, query := range queries {
		_, err := tx.Exec(query)
		require.NoError(t, err)
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
			require.NoError(t, err)
			defer db.Close()

			tx, err := db.Begin()
			require.NoError(t, err)
			defer tx.Rollback()

			setupSqlDb(t, setupQueries, tx)

			rows, err := tx.Query(testCase.query)
			require.NoError(t, err)

			target := reflect.New(testCase.targetType).Interface()
			err = scansion.NewSqlScanner(rows).Scan(target)
			require.NoError(t, err)
			expectedJson, err := json.MarshalIndent(testCase.expected, "", "  ")
			require.NoError(t, err)
			actualJson, err := json.MarshalIndent(target, "", "  ")
			require.NoError(t, err)
			assert.Equal(t, string(expectedJson), string(actualJson))
		})
	}

	t.Run("no_rows", func(t *testing.T) {
		db, err := sql.Open("pgx", dbUrl)
		require.NoError(t, err)
		defer db.Close()

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		setupSqlDb(t, setupQueries, tx)

		rows, err := tx.Query("SELECT * FROM books WHERE 1 = 0 LIMIT 1")
		require.NoError(t, err)

		var book Book
		err = scansion.NewSqlScanner(rows).Scan(&book)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}
