package scansion_test

import (
	"context"
	"os"
	"testing"

	"github.com/dacohen/scansion"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func setupPgxDB(ctx context.Context, t *testing.T, queries []string, tx pgx.Tx) {
	t.Helper()

	for _, query := range queries {
		_, err := tx.Exec(ctx, query)
		assert.NoError(t, err)
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
			assert.NoError(t, err)
			defer db.Close(ctx)

			tx, err := db.Begin(ctx)
			assert.NoError(t, err)
			defer tx.Rollback(ctx)

			setupPgxDB(ctx, t, setupQueries, tx)

			rows, err := tx.Query(ctx, testCase.query)
			assert.NoError(t, err)

			scanner := scansion.NewPgxScanner(rows)
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
