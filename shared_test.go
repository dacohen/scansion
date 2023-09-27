package scansion_test

import (
	"errors"
	"regexp"
)

type Author struct {
	ID        int64   `db:"id,pk"`
	Name      string  `db:"name"`
	Publisher *string `db:"publisher"`

	Books []Book `db:"books"`
}

type MoneyType struct {
	Number   string
	Currency string
}

func (m *MoneyType) Scan(src any) error {
	moneyRegex := regexp.MustCompile(`\((.*),(.*)\)`)
	matches := moneyRegex.FindStringSubmatch(src.(string))
	if len(matches) != 3 {
		return errors.New("invalid money type")
	}

	m.Number = matches[1]
	m.Currency = matches[2]

	return nil
}

type Book struct {
	ID       int64     `db:"id,pk"`
	AuthorID int64     `db:"author_id"`
	Title    string    `db:"title"`
	Price    MoneyType `db:"price"`

	Bookshelves []Bookshelf `db:"bookshelves"`
}

type Bookshelf struct {
	ID   int64  `db:"id,pk"`
	Name string `db:"name"`

	Books []Book `db:"books"`
}

var setupQueries = []string{
	`CREATE TYPE money_type AS (
		number NUMERIC,
		currency TEXT
	)`,
	`CREATE TABLE authors (
		id BIGINT PRIMARY KEY,
		name TEXT NOT NULL,
		publisher TEXT
	)`,
	`CREATE TABLE books (
		id BIGINT PRIMARY KEY,
		author_id BIGINT NOT NULL REFERENCES authors (id),
		title TEXT NOT NULL,
		price money_type NOT NULL
	)`,
	`CREATE TABLE bookshelves (
		id BIGINT PRIMARY KEY,
		name TEXT NOT NULL
	)`,
	`CREATE TABLE books_bookshelves (
		book_id BIGINT NOT NULL REFERENCES books (id),
		bookshelf_id BIGINT NOT NULL REFERENCES bookshelves (id)
	)`,
	`INSERT INTO authors (id, name, publisher)
	VALUES (1, 'Neal Stephenson', 'HarperCollins'),
	(2, 'James Joyce', NULL)`,
	`INSERT INTO books (id, author_id, title, price)
	VALUES (1, 1, 'Cryptonomicon', '(30.00,USD)'),
	(2, 1, 'Snow Crash', '(20.00,USD)'), (3, 2, 'Ulysses', '(25.00,GBP)')`,
	`INSERT INTO bookshelves (id, name) VALUES (1, 'Daniel'), (2, 'George')`,
	`INSERT INTO books_bookshelves (book_id, bookshelf_id) VALUES (1, 1), (2, 1), (3, 1), (3, 2)`,
}

func toPtr[T any](val T) *T {
	return &val
}

var testCases = []struct {
	name     string
	query    string
	manyRows bool
	expected interface{}
}{
	{
		name: "single_root_row",
		query: `SELECT
			authors.*,
			0 AS "scan:books",
			books.*
		FROM authors
		JOIN books ON books.author_id = authors.id
		WHERE authors.id = 1
		ORDER BY authors.id ASC`,
		manyRows: false,
		expected: Author{
			ID:        1,
			Name:      "Neal Stephenson",
			Publisher: toPtr("HarperCollins"),
			Books: []Book{
				{
					ID:       1,
					AuthorID: 1,
					Title:    "Cryptonomicon",
					Price: MoneyType{
						Number:   "30.00",
						Currency: "USD",
					},
				},
				{
					ID:       2,
					AuthorID: 1,
					Title:    "Snow Crash",
					Price: MoneyType{
						Number:   "20.00",
						Currency: "USD",
					},
				},
			},
		},
	},
	{
		name: "multiple_rows",
		query: `SELECT
			authors.*,
			0 AS "scan:books",
			books.*
		FROM authors
		JOIN books ON books.author_id = authors.id
		ORDER BY authors.id ASC`,
		manyRows: true,
		expected: []Author{
			{
				ID:        1,
				Name:      "Neal Stephenson",
				Publisher: toPtr("HarperCollins"),
				Books: []Book{
					{
						ID:       1,
						AuthorID: 1,
						Title:    "Cryptonomicon",
						Price: MoneyType{
							Number:   "30.00",
							Currency: "USD",
						},
					},
					{
						ID:       2,
						AuthorID: 1,
						Title:    "Snow Crash",
						Price: MoneyType{
							Number:   "20.00",
							Currency: "USD",
						},
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
						Price: MoneyType{
							Number:   "25.00",
							Currency: "GBP",
						},
					},
				},
			},
		},
	},
	{
		name: "deep_load",
		query: `SELECT
			authors.*,
			0 AS "scan:books",
			books.*,
			0 AS "scan:books.bookshelves",
			bookshelves.*
		FROM authors
		JOIN books ON books.author_id = authors.id
		JOIN books_bookshelves bbs ON bbs.book_id = books.id
		JOIN bookshelves ON bbs.bookshelf_id = bookshelves.id
		ORDER BY authors.id ASC`,
		manyRows: true,
		expected: []Author{
			{
				ID:        1,
				Name:      "Neal Stephenson",
				Publisher: toPtr("HarperCollins"),
				Books: []Book{
					{
						ID:       1,
						AuthorID: 1,
						Title:    "Cryptonomicon",
						Price: MoneyType{
							Number:   "30.00",
							Currency: "USD",
						},
						Bookshelves: []Bookshelf{
							{
								ID:   1,
								Name: "Daniel",
							},
						},
					},
					{
						ID:       2,
						AuthorID: 1,
						Title:    "Snow Crash",
						Price: MoneyType{
							Number:   "20.00",
							Currency: "USD",
						},
						Bookshelves: []Bookshelf{
							{
								ID:   1,
								Name: "Daniel",
							},
						},
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
						Price: MoneyType{
							Number:   "25.00",
							Currency: "GBP",
						},
						Bookshelves: []Bookshelf{
							{
								ID:   1,
								Name: "Daniel",
							},
							{
								ID:   2,
								Name: "George",
							},
						},
					},
				},
			},
		},
	},
}
