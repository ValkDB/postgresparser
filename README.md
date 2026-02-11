# postgresparser

[![CI](https://github.com/valkdb/postgresparser/actions/workflows/ci.yml/badge.svg)](https://github.com/valkdb/postgresparser/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/valkdb/postgresparser.svg)](https://pkg.go.dev/github.com/valkdb/postgresparser)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A pure-Go PostgreSQL parser. No cgo, no C toolchain — just `go build`.

## Why postgresparser?

Need to parse PostgreSQL SQL in Go but can't use cgo? Deploying to Alpine containers, Lambda, ARM, scratch images, or anywhere that requires `CGO_ENABLED=0`?

`postgresparser` works everywhere `go build` works. It parses SQL into a structured intermediate representation (IR) that gives you tables, columns, joins, filters, CTEs, subqueries, and more — without executing anything.

```go
result, err := postgresparser.ParseSQL(`
    SELECT u.name, COUNT(o.id) AS order_count
    FROM users u
    LEFT JOIN orders o ON o.user_id = u.id
    WHERE u.active = true
    GROUP BY u.name
    ORDER BY order_count DESC
`)

fmt.Println(result.Command)       // "SELECT"
fmt.Println(result.Tables)        // users, orders with aliases
fmt.Println(result.Columns)       // u.name, COUNT(o.id) AS order_count
fmt.Println(result.Where)         // ["u.active=true"]
fmt.Println(result.JoinConditions) // ["o.user_id=u.id"]
fmt.Println(result.GroupBy)       // ["u.name"]
fmt.Println(result.ColumnUsage)   // each column with its role: filter, join, projection, group, order
```

**Performance:** With [SLL prediction mode](docs/performance.md), most queries parse in **70–350 µs**.

## Installation

```bash
go get github.com/valkdb/postgresparser
```

## What you can build with it

- **Query linting** — detect missing WHERE on DELETEs, flag SELECT *, enforce naming conventions
- **Dependency extraction** — map which tables and columns a query touches, build lineage graphs
- **Migration tooling** — parse DDL to understand schema changes, diff CREATE statements
- **Audit logging** — tag log entries with structured metadata (tables, operation type, filtered columns)
- **Query rewriting** — inject tenant filters, add audit columns, transform SQL before execution
- **Index advisors** — analyze column usage patterns to suggest optimal indexes

## Parsing

Handles the SQL you actually write in production:

- **DML**: SELECT, INSERT, UPDATE, DELETE, MERGE
- **DDL**: CREATE TABLE (columns/type/nullability/default), CREATE INDEX, DROP TABLE/INDEX, ALTER TABLE, TRUNCATE
- **CTEs**: `WITH ... AS` including `RECURSIVE`, materialization hints
- **JOINs**: INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, LATERAL
- **Subqueries**: in SELECT, FROM, WHERE, and HAVING
- **Set operations**: UNION, INTERSECT, EXCEPT (ALL/DISTINCT)
- **Upsert**: INSERT ... ON CONFLICT DO UPDATE/DO NOTHING
- **JSONB**: `->`, `->>`, `@>`, `?`, `?|`, `?&`
- **Window functions**: OVER, PARTITION BY
- **Type casts**: `::type`
- **Parameters**: `$1`, `$2`, ...

IR field reference: [ParsedQuery IR Reference](docs/parsed-query.md)

## Supported SQL Statements

See [docs/supported-statements.md](./docs/supported-statements.md) for full details on parsed commands, graceful handling (e.g. SET/SHOW/RESET), and what's currently UNKNOWN or unsupported.

| Category | Statements | Status |
|----------|-----------|--------|
| **DML** | SELECT, INSERT, UPDATE, DELETE, MERGE | Full IR extraction |
| **DDL** | CREATE TABLE, ALTER TABLE, DROP TABLE/INDEX, CREATE INDEX, TRUNCATE | Full IR extraction |
| **Utility** | SET, SHOW, RESET | Graceful — returns `UNKNOWN`, no error |
| **Other** | GRANT, REVOKE, CREATE VIEW/FUNCTION/TRIGGER, COPY, EXPLAIN, VACUUM, BEGIN/COMMIT/ROLLBACK, etc. | Not yet supported — may error or return `UNKNOWN` |

## Analysis

The `analysis` subpackage provides higher-level intelligence on top of the IR:

### Column usage analysis

Know exactly how every column is used — filtering, joining, projection, grouping, ordering:

```go
result, err := analysis.AnalyzeSQL("SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'active'")

for _, cu := range result.ColumnUsage {
    fmt.Printf("%s.%s → %s\n", cu.TableAlias, cu.Column, cu.UsageType)
}
// o.id → projection
// c.name → projection
// o.customer_id → join
// c.id → join
// o.status → filter
```

### WHERE condition extraction

Pull structured conditions with operators and values:

```go
conditions, _ := analysis.ExtractWhereConditions("SELECT * FROM orders WHERE status = 'active' AND total > 100")

for _, c := range conditions {
    fmt.Printf("%s %s %v\n", c.Column, c.Operator, c.Value)
}
// status = active
// total > 100
```

### Schema-aware JOIN relationship detection

Pass in your schema metadata and get back foreign key relationships — no heuristic guessing:

```go
schema := map[string][]analysis.ColumnSchema{
    "customers": {
        {Name: "id", PGType: "bigint", IsPrimaryKey: true},
        {Name: "name", PGType: "text"},
    },
    "orders": {
        {Name: "id", PGType: "bigint", IsPrimaryKey: true},
        {Name: "customer_id", PGType: "bigint"},
    },
}

joins, _ := analysis.ExtractJoinRelationshipsWithSchema(
    "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id",
    schema,
)
// orders.customer_id → customers.id
```

### DDL extraction

For `CREATE TABLE` parsing, see [`examples/ddl/`](examples/ddl/).

## Performance

With SLL prediction mode, `postgresparser` parses most queries in **70–350 µs** with minimal allocations. The IR extraction layer accounts for only ~3% of CPU — the rest is ANTLR's grammar engine, which SLL mode keeps fast.

See the [Performance Guide](docs/performance.md) for benchmarks, profiling results, and optimization details.

## Examples

See the [`examples/`](examples/) directory:

- [`basic/`](examples/basic/) — Parse SQL and inspect the IR
- [`analysis/`](examples/analysis/) — Column usage, WHERE conditions, JOIN relationships
- [`ddl/`](examples/ddl/) — Parse CREATE TABLE / ALTER TABLE plus DELETE command metadata
- [`sll_mode/`](examples/sll_mode/) — SLL prediction mode for maximum throughput

## Grammar

Built on ANTLR4 grammar files in `grammar/`. To regenerate after modifying:

```bash
antlr4 -Dlanguage=Go -visitor -listener -package gen -o gen grammar/PostgreSQLLexer.g4 grammar/PostgreSQLParser.g4
```

## Compatibility

This is an ANTLR4-based grammar, not PostgreSQL's internal server parser. Some edge-case syntax may differ across PostgreSQL versions. If you find a query that parses in PostgreSQL but fails here, please [open an issue](https://github.com/valkdb/postgresparser/issues) with a minimal repro.

`ParseSQL` processes the first SQL statement. Multi-statement strings (separated by `;`) will have subsequent statements silently ignored.

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
