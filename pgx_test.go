package scansion_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/dacohen/scansion"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupPgxDB(ctx context.Context, t *testing.T, queries []string, tx pgx.Tx) {
	t.Helper()

	for _, query := range queries {
		_, err := tx.Exec(ctx, query)
		require.NoError(t, err)
	}
}

func TestPgxScan(t *testing.T) {
	dbUrl, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
		dbUrl = "host=localhost user=postgres dbname=scansion_test"
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := pgx.Connect(ctx, dbUrl)
			require.NoError(t, err)
			defer db.Close(ctx)

			tx, err := db.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			setupPgxDB(ctx, t, setupQueries, tx)

			rows, err := tx.Query(ctx, testCase.query)
			require.NoError(t, err)

			target := reflect.New(testCase.targetType).Interface()
			err = scansion.NewPgxScanner(rows).Scan(target)
			require.NoError(t, err)
			expectedJson, err := json.MarshalIndent(testCase.expected, "", "  ")
			require.NoError(t, err)
			actualJson, err := json.MarshalIndent(target, "", "  ")
			require.NoError(t, err)
			assert.Equal(t, string(expectedJson), string(actualJson))
		})
	}

	t.Run("no_rows", func(t *testing.T) {
		ctx := context.Background()
		db, err := pgx.Connect(ctx, dbUrl)
		require.NoError(t, err)
		defer db.Close(ctx)

		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		setupPgxDB(ctx, t, setupQueries, tx)

		rows, err := tx.Query(ctx, "SELECT * FROM books WHERE 1 = 0 LIMIT 1")
		require.NoError(t, err)

		var book Book
		err = scansion.NewPgxScanner(rows).Scan(&book)
		require.ErrorIs(t, err, pgx.ErrNoRows)
	})
}

func BenchmarkPgxScan(b *testing.B) {
	dbUrl, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
		dbUrl = "host=localhost user=postgres dbname=scansion_test"
	}

	ctx := context.Background()
	db, err := pgx.Connect(ctx, dbUrl)
	require.NoError(b, err)
	defer db.Close(ctx)

	type testStruct struct {
		Value int `db:"value,pk"`
	}

	for b.Loop() {
		tx, err := db.Begin(ctx)
		require.NoError(b, err)

		rows, err := tx.Query(ctx, "SELECT generate_series AS value FROM generate_series(1, 10000)")
		require.NoError(b, err)

		var numbers []testStruct
		err = scansion.NewPgxScanner(rows).Scan(&numbers)
		require.NoError(b, err)

		tx.Rollback(ctx)
	}
}
