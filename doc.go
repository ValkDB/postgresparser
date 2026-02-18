// Package postgresparser provides an ANTLR4-based PostgreSQL SQL parser
// that converts SQL text into a structured intermediate representation (IR).
//
// The parser supports SELECT, INSERT, UPDATE, DELETE, and MERGE statements
// with full support for CTEs, subqueries, JOINs, set operations (UNION,
// INTERSECT, EXCEPT), window functions, and JSONB operators.
//
// # Quick Start
//
// Parse a SQL query into a structured ParsedQuery:
//
//	result, err := postgresparser.ParseSQL("SELECT id, name FROM users WHERE active = true")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result.Command)  // "SELECT"
//	fmt.Println(result.Tables)   // [{Schema:"" Name:"users" Alias:"" Type:"base" Raw:"users"}]
//	fmt.Println(result.Columns)  // [{Expression:"id" Alias:""}, {Expression:"name" Alias:""}]
//
// ParseSQL parses only the first statement in a multi-statement input for
// backward compatibility. Use ParseSQLAll to parse all statements with explicit
// per-statement status/warnings, or ParseSQLStrict to fail unless exactly one
// statement is present.
//
// # Analysis Subpackage
//
// The analysis subpackage provides higher-level SQL analysis on top of the
// core parser, including:
//
//   - Column usage analysis (which columns are used for filtering, joining, ordering)
//   - WHERE condition extraction with operator and value details
//   - JOIN relationship inference (parent-child table detection)
//   - Schema-aware FK detection using primary key metadata
//
// Example:
//
//	result, err := analysis.AnalyzeSQL("SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'active'")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result.Tables)       // tables referenced
//	fmt.Println(result.ColumnUsage)  // columns with usage types
//
// # Supported SQL Features
//
//   - SELECT with projections, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET
//   - INSERT with ON CONFLICT (upsert), RETURNING
//   - UPDATE with FROM clause, RETURNING
//   - DELETE with USING clause, RETURNING
//   - MERGE with MATCHED/NOT MATCHED actions
//   - CREATE TABLE with column metadata (name, type, nullability, default)
//   - CREATE/DROP INDEX, DROP TABLE, ALTER TABLE, TRUNCATE
//   - Common Table Expressions (WITH ... AS)
//   - Subqueries in SELECT, FROM, WHERE, and HAVING
//   - All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, LATERAL)
//   - Set operations (UNION, INTERSECT, EXCEPT with ALL/DISTINCT)
//   - JSONB operators (->>, ->, @>, ?, ?|, ?&)
//   - Type casts (::type)
//   - Window functions (OVER, PARTITION BY)
//   - Parameter placeholders ($1, $2, ...)
package postgresparser
