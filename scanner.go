package scansion

type Scanner interface {
	Scan(v interface{}) error
}

type scannerFunc func(i ...interface{}) error
