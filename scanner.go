// Scansion is a library for scanning SQL result sets into Go structs.
package scansion

// Scanner is generic interface for scanning from a DB.
// All supported library-specific scanners implement this.
type Scanner interface {
	Scan(v any) error
}
