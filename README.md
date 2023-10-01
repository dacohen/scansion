# Scansion

Scansion is a library for scanning SQL result sets into Go structs.
Scansion currently supports the following libraries:
* [pgx](https://github.com/jackc/pgx) (pgx.Rows)
* stdlib `database/sql` (*sql.Rows)

How you generate your SQL is up to you.
You can use an ORM, a query builder (e.g. [squirrel](https://github.com/Masterminds/squirrel)), or write it by hand. Scansion will process the results

Scansion supports:
* Nested structs
* One-to-many relationships

## Example
```go
package main

import (
    "context"
    "log"

    "github.com/dacohen/scansion"
    "github.com/jackc/pgx/v5"
)

type Author struct {
	ID        int64   `db:"id,pk"`
	Name      string  `db:"name"`
	Publisher *string `db:"publisher"`

	Books []Book `db:"books"`
}

type Book struct {
	ID       int64     `db:"id,pk"`
	AuthorID int64     `db:"author_id"`
	Title    string    `db:"title"`

	Bookshelves []Bookshelf `db:"bookshelves"`
}

func main() {
    ctx := context.Background()
    conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatalf("Unable to connect to DB: %s", err)
    }
    defer conn.Close(ctx)

    query := `
    SELECT
        authors.*,
        0 AS "scan:books",
        books.*
    FROM authors
    JOIN books ON books.author_id = authors.id
    WHERE authors.id = 1
    ORDER BY authors.id ASC`

    rows, err := conn.Query(ctx, query)
    if err != nil {
        log.Fatalf("Error executing query: %s", err)
    }
    
    var authors []Author
    scanner := scansion.NewScanner(rows)
    err = scanner.Scan(&authors)
    if err != nil {
        log.Fatalf("Error scanning result: %s", err)
    }
}


```

## Key ideas

### Struct tags
Scansion relies on a `db` struct tag to determine how to map results to structs.
Most fields will only specify the name of their corresponding column in the result set:

```go
type Author struct {
    // ...
    Name string `db:"name"`
    // ...
}
```

However, each struct needs exactly *one* "primary key" specified:

```go
type Author struct {
    // ...
    ID int64 `db:"id,pk"`
    // ...
}
```

A primary key is notated, by adding `,pk` to the end of the `db` tag.

This primary key is a column that uniquely identifies an instance of that struct in the results.
This is often called `id` or similar.
This does not need to be an actual Primary Key in your database, although since they serve a similar purpose, it often will be.

### Scan columns
The SQL standard doesn't provide a mechanism for natively determining the boundary between tables.
For example:
```sql
SELECT table_a.*, table_b.* FROM table_a JOIN table_b;
```
If `table_a` and `table_b` both have an `id` column, the result set doesn't disambiguate, as the prefix `table_a.*` is elided on return.

To solve this, scansion requires delineating the boundary between tables in a result set with a special, zero column:

```sql
SELECT
    table_a.*,
    0 as "scan:table_b",
    table_b.*,
FROM table_a
JOIN table_b
```