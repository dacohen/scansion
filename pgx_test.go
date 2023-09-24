package sqlscan_test

import (
	"context"
	"os"
	"testing"

	"github.com/dacohen/sqlscan"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

type Author struct {
	ID        int64   `db:"id"`
	Name      string  `db:"name"`
	Publisher *string `db:"publisher"`

	Books []Book `db:"books"`
}

type Book struct {
	ID       int64  `db:"id"`
	AuthorID int64  `db:"author_id"`
	Title    string `db:"title"`
}

func setupDB(ctx context.Context, t *testing.T, tx pgx.Tx) {
	t.Helper()

	_, err := tx.Exec(ctx, `CREATE TABLE authors (
		id BIGINT PRIMARY KEY,
		name TEXT NOT NULL,
		publisher TEXT
	)`)
	assert.NoError(t, err)

	_, err = tx.Exec(ctx, `CREATE TABLE books (
		id BIGINT PRIMARY KEY,
		author_id BIGINT NOT NULL REFERENCES authors (id),
		title TEXT NOT NULL
	)`)
	assert.NoError(t, err)

	_, err = tx.Exec(ctx, `INSERT INTO authors (id, name, publisher)
		VALUES (1, 'Neal Stephenson', 'HarperCollins'),
		(2, 'James Joyce', NULL)`)
	assert.NoError(t, err)

	_, err = tx.Exec(ctx, `INSERT INTO books (id, author_id, title)
		VALUES (1, 1, 'Cryptonomicon'), (2, 1, 'Snow Crash'), (3, 2, 'Ulysses')`)
	assert.NoError(t, err)
}

func TestPgxScan(t *testing.T) {

	dbUrl, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
		dbUrl = "host=localhost user=postgres dbname=sqlscan_test"
	}

	t.Run("single row", func(t *testing.T) {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, dbUrl)
		assert.NoError(t, err)
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		assert.NoError(t, err)
		defer tx.Rollback(ctx)

		setupDB(ctx, t, tx)

		rows, err := tx.Query(ctx, `SELECT
			authors.*,
			0 AS "scan:many(books, id)",
			books.*
		FROM authors
		JOIN books ON books.author_id = authors.id
		WHERE authors.id = 1
		ORDER BY authors.id ASC`)
		assert.NoError(t, err)

		var author Author
		scanner := sqlscan.NewPgxScanner(rows)
		err = scanner.Scan(&author)
		assert.NoError(t, err)

		publisher := "HarperCollins"
		expectedResult := Author{
			ID:        1,
			Name:      "Neal Stephenson",
			Publisher: &publisher,
			Books: []Book{
				{
					ID:       1,
					AuthorID: 1,
					Title:    "Cryptonomicon",
				},
				{
					ID:       2,
					AuthorID: 1,
					Title:    "Snow Crash",
				},
			},
		}

		assert.EqualValues(t, expectedResult, author)
	})

	t.Run("multiple rows", func(t *testing.T) {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, dbUrl)
		assert.NoError(t, err)
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		assert.NoError(t, err)
		defer tx.Rollback(ctx)

		setupDB(ctx, t, tx)

		rows, err := tx.Query(ctx, `SELECT
			authors.*,
			0 AS "scan:many(books, id)",
			books.*
		FROM authors
		JOIN books ON books.author_id = authors.id
		ORDER BY authors.id ASC`)
		assert.NoError(t, err)

		var authors []Author
		scanner := sqlscan.NewPgxScanner(rows)
		err = scanner.Scan(&authors)
		assert.NoError(t, err)

		publisher := "HarperCollins"
		expectedResult := []Author{
			{
				ID:        1,
				Name:      "Neal Stephenson",
				Publisher: &publisher,
				Books: []Book{
					{
						ID:       1,
						AuthorID: 1,
						Title:    "Cryptonomicon",
					},
					{
						ID:       2,
						AuthorID: 1,
						Title:    "Snow Crash",
					},
				},
			},
			{
				ID:   2,
				Name: "James Joyce",
				Books: []Book{
					{
						ID:       3,
						AuthorID: 2,
						Title:    "Ulysses",
					},
				},
			},
		}

		assert.EqualValues(t, expectedResult, authors)
	})
}
