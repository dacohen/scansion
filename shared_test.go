package scansion_test

import (
	"errors"
	"reflect"
	"regexp"
	"time"
)

type Timestamps struct {
	CreatedAt time.Time  `db:"created_at"`
	UpdatedAt *time.Time `db:"updated_at"`
}

type Website struct {
	ID  int64  `db:"id,pk"`
	URL string `db:"url"`

	Author Author `db:"author"`
}

type Author struct {
	ID         int64    `db:"id,pk"`
	Name       string   `db:"name"`
	Publisher  *string  `db:"publisher"`
	HometownID *int64   `db:"hometown_id"`
	Hometown   *City    `db:"hometown"`
	WebsiteID  *int64   `db:"website_id"`
	Website    *Website `db:"website"`

	Books []Book `db:"books"`

	Timestamps
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

type City struct {
	ID      int64  `db:"id,pk"`
	Name    string `db:"name"`
	Country string `db:"country"`
}

type Book struct {
	ID       int64     `db:"id,pk"`
	AuthorID int64     `db:"author_id"`
	Title    string    `db:"title"`
	Price    MoneyType `db:"price,flat"`

	Author      Author      `db:"authors"`
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
	`CREATE TABLE cities (
		id BIGINT PRIMARY KEY,
		name TEXT NOT NULL,
		country TEXT NOT NULL
	)`,
	`CREATE TABLE websites (
		id BIGINT PRIMARY KEY,
		url TEXT NOT NULL
	)`,
	`CREATE TABLE authors (
		id BIGINT PRIMARY KEY,
		name TEXT NOT NULL,
		publisher TEXT,
		hometown_id BIGINT REFERENCES cities (id),
		website_id BIGINT REFERENCES websites (id),
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMPTZ
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
	`INSERT INTO cities (id, name, country) VALUES
	(1, 'Dublin', 'Ireland')`,
	`INSERT INTO websites (id, url) VALUES (1, 'https://nealstephenson.com/')`,
	`INSERT INTO authors (id, name, publisher, hometown_id, website_id, created_at)
	VALUES (1, 'Neal Stephenson', 'HarperCollins', NULL, 1, '2023-01-02 15:04:05 UTC'),
	(2, 'James Joyce', NULL, 1, NULL, '2023-01-02 15:04:05 UTC')`,
	`INSERT INTO books (id, author_id, title, price)
	VALUES (1, 1, 'Cryptonomicon', '(30.00,USD)'),
	(2, 1, 'Snow Crash', '(20.00,USD)'), (3, 2, 'Ulysses', '(25.00,GBP)')`,
	`INSERT INTO bookshelves (id, name) VALUES (1, 'Daniel'), (2, 'George')`,
	`INSERT INTO books_bookshelves (book_id, bookshelf_id) VALUES (1, 1), (2, 1), (3, 1), (3, 2)`,
}

func toPtr[T any](val T) *T {
	return &val
}

func getCreatedAt() time.Time {
	createdAt, _ := time.Parse(time.RFC3339, `2023-01-02T15:04:05Z`)
	return createdAt.In(time.Local)
}

var testCases = []struct {
	name       string
	query      string
	targetType reflect.Type
	expected   any
}{
	{
		name: "single_root_row",
		query: `SELECT
			authors.*,
			0 AS "scan:books",
			books.*,
			0 AS "scan:hometown",
			cities.*,
			0 AS "scan:website",
			websites.*
		FROM authors
		JOIN books ON books.author_id = authors.id
		LEFT JOIN cities ON authors.hometown_id = cities.id
		LEFT JOIN websites ON authors.website_id = websites.id
		WHERE authors.id = 1
		ORDER BY authors.id ASC`,
		targetType: reflect.TypeOf(Author{}),
		expected: Author{
			ID:        1,
			Name:      "Neal Stephenson",
			Publisher: toPtr("HarperCollins"),
			WebsiteID: toPtr(int64(1)),
			Website: &Website{
				ID:  1,
				URL: "https://nealstephenson.com/",
			},
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
			Timestamps: Timestamps{
				CreatedAt: getCreatedAt(),
			},
		},
	},
	{
		name: "multiple_rows",
		query: `SELECT
			authors.*,
			0 AS "scan:books",
			books.*,
			0 AS "scan:hometown",
			cities.*,
			0 AS "scan:website",
			websites.*
		FROM authors
		JOIN books ON books.author_id = authors.id
		LEFT JOIN cities ON authors.hometown_id = cities.id
		LEFT JOIN websites ON authors.website_id = websites.id
		ORDER BY authors.id ASC`,
		targetType: reflect.TypeOf([]Author{}),
		expected: &[]Author{
			{
				ID:        1,
				Name:      "Neal Stephenson",
				Publisher: toPtr("HarperCollins"),
				WebsiteID: toPtr(int64(1)),
				Website: &Website{
					ID:  1,
					URL: "https://nealstephenson.com/",
				},
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
				Timestamps: Timestamps{
					CreatedAt: getCreatedAt(),
				},
			},
			{
				ID:         2,
				Name:       "James Joyce",
				HometownID: toPtr(int64(1)),
				Hometown: &City{
					ID:      1,
					Name:    "Dublin",
					Country: "Ireland",
				},
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
				Timestamps: Timestamps{
					CreatedAt: getCreatedAt(),
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
			bookshelves.*,
			0 AS "scan:hometown",
			cities.*,
			0 AS "scan:website",
			websites.*
		FROM authors
		LEFT JOIN cities ON authors.hometown_id = cities.id
		LEFT JOIN websites ON authors.website_id = websites.id
		JOIN books ON books.author_id = authors.id
		JOIN books_bookshelves bbs ON bbs.book_id = books.id
		JOIN bookshelves ON bbs.bookshelf_id = bookshelves.id
		ORDER BY authors.id ASC`,
		targetType: reflect.TypeOf([]Author{}),
		expected: &[]Author{
			{
				ID:        1,
				Name:      "Neal Stephenson",
				Publisher: toPtr("HarperCollins"),
				WebsiteID: toPtr(int64(1)),
				Website: &Website{
					ID:  1,
					URL: "https://nealstephenson.com/",
				},
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
				Timestamps: Timestamps{
					CreatedAt: getCreatedAt(),
				},
			},
			{
				ID:         2,
				Name:       "James Joyce",
				HometownID: toPtr(int64(1)),
				Hometown: &City{
					ID:      1,
					Name:    "Dublin",
					Country: "Ireland",
				},
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
				Timestamps: Timestamps{
					CreatedAt: getCreatedAt(),
				},
			},
		},
	},
	{
		name: "by_book",
		query: `SELECT books.*, 0 AS "scan:authors", authors.*, 0 AS "scan:authors.website", websites.*
		FROM books
		JOIN authors ON books.author_id = authors.id
		LEFT JOIN websites ON authors.website_id = websites.id
		ORDER BY books.id ASC`,
		targetType: reflect.TypeOf([]Book{}),
		expected: []Book{
			{
				ID:       1,
				AuthorID: 1,
				Title:    "Cryptonomicon",
				Price: MoneyType{
					Number:   "30.00",
					Currency: "USD",
				},
				Author: Author{
					ID:        1,
					Name:      "Neal Stephenson",
					Publisher: toPtr("HarperCollins"),
					WebsiteID: toPtr(int64(1)),
					Website: &Website{
						ID:  1,
						URL: "https://nealstephenson.com/",
					},
					Timestamps: Timestamps{
						CreatedAt: getCreatedAt(),
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
				Author: Author{
					ID:        1,
					Name:      "Neal Stephenson",
					Publisher: toPtr("HarperCollins"),
					WebsiteID: toPtr(int64(1)),
					Website: &Website{
						ID:  1,
						URL: "https://nealstephenson.com/",
					},
					Timestamps: Timestamps{
						CreatedAt: getCreatedAt(),
					},
				},
			},
			{
				ID:       3,
				AuthorID: 2,
				Title:    "Ulysses",
				Price: MoneyType{
					Number:   "25.00",
					Currency: "GBP",
				},
				Author: Author{
					ID:         2,
					Name:       "James Joyce",
					HometownID: toPtr(int64(1)),
					Timestamps: Timestamps{
						CreatedAt: getCreatedAt(),
					},
				},
			},
		},
	},
	{
		name: "recursive_slices",
		query: `SELECT websites.*,
			0 AS "scan:author",
			authors.*,
			0 AS "scan:author.books",
			books.*
		FROM websites
		JOIN authors ON authors.website_id = websites.id
		JOIN books ON books.author_id = authors.id
		WHERE websites.id = 1`,
		targetType: reflect.TypeOf(Website{}),
		expected: &Website{
			ID:  1,
			URL: "https://nealstephenson.com/",
			Author: Author{
				ID:        1,
				Name:      "Neal Stephenson",
				Publisher: toPtr("HarperCollins"),
				WebsiteID: toPtr(int64(1)),
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
				Timestamps: Timestamps{
					CreatedAt: getCreatedAt(),
				},
			},
		},
	},
}
