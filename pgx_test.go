package sqlscan_test

import (
	"context"
	"testing"

	"github.com/dacohen/sqlscan"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

type Author struct {
	ID        int64   `db:"id"`
	Name      string  `db:"name"`
	Publisher *string `db:"publisher"`
}

type Book struct {
	ID       int64  `db:"id"`
	AuthorID int64  `db:"author_id"`
	Title    string `db:"name"`
}

func setupDB(ctx context.Context, t *testing.T, tx pgx.Tx) {
	t.Helper()

	_, err := tx.Exec(ctx, `CREATE TABLE authors (
		id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		name TEXT NOT NULL,
		publisher TEXT
	)`)
	assert.NoError(t, err)

	_, err = tx.Exec(ctx, `CREATE TABLE books (
		id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		author_id BIGINT NOT NULL REFERENCES authors (id),
		title TEXT NOT NULL
	)`)
	assert.NoError(t, err)

	_, err = tx.Exec(ctx, `INSERT INTO authors (name, publisher)
		VALUES ('Neal Stephenson', 'HarperCollins'),
		('James Joyce', NULL)`)
	assert.NoError(t, err)
}

func TestPgxScan(t *testing.T) {
	t.Run("single row", func(t *testing.T) {
		ctx := context.Background()
		dbUrl := "host=localhost user=postgres dbname=sqlscan_test"
		conn, err := pgx.Connect(ctx, dbUrl)
		assert.NoError(t, err)
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		assert.NoError(t, err)
		defer tx.Rollback(ctx)

		setupDB(ctx, t, tx)

		rows, err := tx.Query(ctx, `SELECT * FROM authors ORDER BY id ASC LIMIT 1`)
		assert.NoError(t, err)

		var author Author
		scanner := sqlscan.NewPgxScanner(rows)
		err = scanner.Scan(&author)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), author.ID)
		assert.Equal(t, "Neal Stephenson", author.Name)
		assert.Equal(t, "HarperCollins", *author.Publisher)
	})

	t.Run("multiple rows", func(t *testing.T) {
		ctx := context.Background()
		dbUrl := "host=localhost user=postgres dbname=sqlscan_test"
		conn, err := pgx.Connect(ctx, dbUrl)
		assert.NoError(t, err)
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		assert.NoError(t, err)
		defer tx.Rollback(ctx)

		setupDB(ctx, t, tx)

		rows, err := tx.Query(ctx, `SELECT * FROM authors ORDER BY id ASC`)
		assert.NoError(t, err)

		var authors []Author
		scanner := sqlscan.NewPgxScanner(rows)
		err = scanner.Scan(&authors)
		assert.NoError(t, err)
		assert.Len(t, authors, 2)
		assert.Equal(t, int64(1), authors[0].ID)
		assert.Equal(t, "Neal Stephenson", authors[0].Name)
		assert.NotNil(t, authors[0].Publisher)
		assert.Equal(t, int64(2), authors[1].ID)
		assert.Equal(t, "James Joyce", authors[1].Name)
		assert.Nil(t, authors[1].Publisher)
	})
}
