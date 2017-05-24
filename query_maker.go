package jsq

// QueryMaker defines an interface for JSQL implementations
type QueryMaker interface {

	// Parse builds the query
	Parse(jsonQuery string) error

	// Set the table to query
	SetTable(table interface{}, plural bool)

	// Find the records matching the prepared query
	Find(out interface{}, op ...QueryOption) error

	// Count counts the number of records matching the prepared query
	Count() (int64, error)
}
