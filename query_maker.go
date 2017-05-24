package jsq

// QueryMaker defines an interface for JSQL implementations
type QueryMaker interface {

	// Parse builds the query
	Parse(jsonQuery string) error

	// Set the table to query
	SetTable(table interface{})

	// First returns the first record matching the prepared query
	First(out interface{}, op ...QueryOption) error

	// Last returns the last record matching the prepared query
	Last(out interface{}, op ...QueryOption) error

	// All returns all the records matching the prepared query
	All(out interface{}, op ...QueryOption) error

	// Count counts the number of records matching the prepared query
	Count() (int64, error)
}
