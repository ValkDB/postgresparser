package postgresparser

// ParseOptions controls optional metadata extraction during parsing.
type ParseOptions struct {
	// IncludeCreateTableFieldComments enables extraction of line comments (`-- ...`)
	// that immediately precede CREATE TABLE column definitions.
	IncludeCreateTableFieldComments bool
}
