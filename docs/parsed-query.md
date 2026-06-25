# ParsedQuery IR Reference

This document defines how to interpret parser outputs:
- `postgresparser.ParseSQL` / `postgresparser.ParseSQLStrict` output (`ParsedQuery` in `ir.go`)
- `postgresparser.ParseSQLAll` output (`ParseBatchResult` in `ir.go`)
- Options-enabled variants:
  - `postgresparser.ParseSQLWithOptions`
  - `postgresparser.ParseSQLAllWithOptions`
  - `postgresparser.ParseSQLStrictWithOptions`

## Purpose

`ParsedQuery` is an analysis-oriented intermediate representation (IR), not a full PostgreSQL AST.

It is designed for:
- query linting
- dependency extraction
- lineage and metadata tooling
- migration and DDL inspection

It is not designed for:
- lossless round-trip SQL generation
- preserving every grammar-level node detail

## Scope

- `ParseSQL` parses only the first statement in the input string.
- `ParseSQLAll` parses all statements in the input string and returns `ParseBatchResult`.
- `ParseSQLStrict` returns an error unless exactly one statement is present.
- `ParseSQLWithOptions` / `ParseSQLAllWithOptions` / `ParseSQLStrictWithOptions` behave identically while enabling optional metadata extraction flags.
- Unrelated sections are expected to be empty for a given command.
- `Command` is the primary discriminator for which sections to read.

## ParseBatchResult

`ParseBatchResult` fields:
- `Statements`: One `StatementParseResult` per input statement in source order.
  - `Index`: 1-based input statement index.
  - `RawSQL`: statement-scoped SQL text.
  - `Query`: parsed IR when statement conversion succeeds (`nil` on failure).
  - `Warnings`: statement-scoped warnings (`SYNTAX_ERROR`).
- `HasFailures`: `true` when any statement has a nil `Query` or any `Warnings`.

## Core Envelope

- `Command`: High-level statement type (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DDL`, `UNKNOWN`).
- `RawSQL`: Preprocessed SQL string used for parsing.
- `Parameters`: Positional/anonymous parameter placeholders (`$1`, `?`, etc.).
- `Placeholders`: Placeholder occurrences in source order with syntactic roles such as `where_value`, `function_arg`, `group_by_ordinal`, `order_by_ordinal`, `limit`, `offset`, `interval_operand`, `insert_value`, and `update_set_value`.

## Relation Metadata

- `Tables`: Structured relation refs (`Schema`, `Name`, `Alias`, `Type`, `Raw`). `Nested` is true for relations surfaced from inside a CTE or subquery body rather than the query's own FROM.
- `CTEs`: `WITH` definitions.
  - `Name`: CTE binding name.
  - `Query`: raw SQL text of the CTE body.
  - `ParsedQuery`: nested IR for the CTE body when the body is a supported preparable statement.
  - `Materialized`: materialization hint (`MATERIALIZED`, `NOT MATERIALIZED`, or empty).
- `Subqueries`: Nested query refs discovered in the statement.
  - `Alias`: binding name for FROM-clause derived tables (empty for expression subqueries).
  - `SourceClause`: clause the subquery was found in (`WHERE`, `HAVING`, `SELECT`, `FROM`, or `SETOP`).
  - `Query`: nested IR for the subquery body.
- `JoinConditions`: Raw join condition expressions.
- `Correlations`: Outer/inner alias correlation metadata for lateral/correlated subqueries.

## Read-Query Shape

- `Columns`: Projection expressions and aliases.
- `ColumnUsage`: Expression-level column usage classification. See [Column Usage](#column-usage) for the per-field contract.
- `Where`: WHERE/CURRENT clauses as raw expressions.
- `Having`: HAVING clauses.
- `GroupBy`: GROUP BY expressions.
- `OrderBy`: ORDER BY expressions + direction/nulls modifiers.
- `Limit`: LIMIT/OFFSET metadata.
- `SetOperations`: UNION/INTERSECT/EXCEPT branches.
- `DerivedColumns`: Alias-to-expression map for derived projection columns.

### Column Usage

`ColumnUsage` describes a single reference to a column in the parsed statement. Beyond the basic fields (`TableAlias`, `Column`, `Expression`, `UsageType`, `Operator`, `Side`, `Context`), two fields surface function-call context around the column:

- `Functions []string` — function names that wrap the column reference outside WHERE clauses (SELECT projection, ORDER BY, GROUP BY, HAVING, etc.). Listed in source order from outermost to innermost. Empty on WHERE-clause entries.
- `Function *FunctionWrapper` — populated only on WHERE-clause filter usages whose subject column is wrapped by an allowlisted function. Nil otherwise.

The asymmetry is deliberate: `Functions []string` is sufficient for projection-level introspection (a SQL formatter or column-rename tool), while `Function *FunctionWrapper` carries the typed metadata needed for predicate-level reasoning (a query rewriter or simulation tool that needs to know exactly which function wraps a filter column and what its other arguments are).

#### `FunctionWrapper`

```go
type FunctionWrapper struct {
    Name     string         // canonical lowercase, unqualified
    Schema   string         // "" for unqualified or pg_catalog (canonicalised)
    Args     []FunctionArg  // literal arguments other than the column itself
    IsNested bool           // true when reached through additional wrappers
    Cast     string         // outermost cast target type, "" if no cast
}
```

The allowlist that triggers attribution is exactly eight functions: `length`, `lower`, `upper`, `coalesce`, `extract`, `date_trunc`, `char_length`, `octet_length`. These are the function calls whose presence in a predicate position changes the comparison's semantics in a way downstream consumers (ORMs, linters, query rewriters, AI-assisted SQL generators) typically key off.

Attribution rules:

- **Scope is WHERE-equivalent only.** WHERE clauses, the WHERE clause of UPDATE/DELETE, the WHERE clause inside CTE bodies, the WHERE clause inside subqueries, and the WHERE clause in each branch of UNION/INTERSECT/EXCEPT all attach wrappers. JOIN ON, ORDER BY, GROUP BY, HAVING, window PARTITION/ORDER, RETURNING, and SELECT projection do not — those use `Functions []string` only.
- **Schema canonicalization.** Bare names and `pg_catalog.<name>` calls produce wrappers with `Schema = ""`. Any other schema (`public.foo(...)`, `myschema.length(...)`) rejects the wrapper outright.
- **Outermost-only attribution.** When wrappers nest (`lower(lower(col))`, `length(lower(col))`), `Name` reflects the outermost call and `IsNested = true`. The inner chain is observable only via the flag, not enumerated.
- **Casts.** A typecast around the wrapper as a whole (`length(col)::int`, `CAST(length(col) AS bigint)`) is captured in `Cast` as the textual target type. Chained casts record the outermost. A cast on the bare column (`col::int < 5`) does not produce a wrapper.
- **Expression-wrapped columns are skipped.** When the function argument is itself an expression (`length(col || 'x')`, `lower(coalesce(name, ''))`), no wrapper is attached. Consumers needing this case should fall through to the standard column+operator interpretation.

#### `FunctionArg`

```go
type FunctionArg struct {
    Literal *string  // SQL textual form; nil = non-literal expression
    IsNull  bool     // explicit SQL NULL keyword
}
```

`FunctionArg` is a three-state encoding:

- `Literal` non-nil, `IsNull` false → literal value present. Numerics, booleans, and intervals are stringified (`"0"`, `"true"`, `"1 day"`); string literals retain their surrounding quotes.
- `Literal` nil, `IsNull` true → explicit `NULL` keyword.
- `Literal` nil, `IsNull` false → non-literal expression at this position (column reference, sub-call, placeholder).

Consumers requiring a specific literal value (`date_trunc` unit, `extract` field) MUST nil-check `Literal` before dereferencing, and fall back to the standard column+operator interpretation when nil.

## DML Shape

- `InsertColumns`: Target columns for INSERT.
- `SetClauses`: SET clauses for UPDATE (and related clause extraction).
- `Returning`: RETURNING clauses.
- `Upsert`: `ON CONFLICT` metadata for INSERT.
- `Merge`: MERGE metadata (target/source/condition/actions).

## DDL Shape

- `DDLActions`: Normalized DDL actions extracted from DDL statements.

Common DDL action fields:
- `Type`: `CREATE_TABLE`, `DROP_TABLE`, `DROP_COLUMN`, `ALTER_TABLE`, `CREATE_INDEX`, `DROP_INDEX`, `TRUNCATE`, `COMMENT`.
- `ObjectName`: Unqualified target object identifier.
- `ObjectType`: Object category for action-specific handling (for example `TABLE`, `COLUMN`, `INDEX` on `COMMENT` actions).
- `Schema`: Parsed schema when available.
- `Columns`: Column names or indexed expressions relevant to the action.
- `Flags`: Modifiers like `IF_EXISTS`, `IF_NOT_EXISTS`, `CASCADE`, `CONCURRENTLY`, etc.
- `IndexType`: Index method for `CREATE_INDEX` (for example `btree`, `gin`).
- `IncludeColumns`: Non-key columns from `CREATE INDEX ... INCLUDE (...)`. Empty for indexes without an `INCLUDE` clause.
- `Predicate`: Partial-index expression from `CREATE INDEX ... WHERE ...`, captured as text without the leading `WHERE` keyword. Empty for non-partial indexes.
- `ColumnDetails`: Column metadata for `CREATE_TABLE` actions.
- `Constraints`: Optional `*DDLConstraints` grouping PK/FK/UNIQUE/CHECK metadata (`CREATE_TABLE`, `ALTER_TABLE ADD CONSTRAINT`).
- `Target`: Generic fully-qualified target path for comment-like actions (for example `public.users.email`).
- `Comment`: Comment text for `COMMENT` actions.

`ColumnDetails` (`[]DDLColumn`) fields:
- `Name`
- `Type`
- `Nullable`
- `Default`
- `Comment` (`[]string`, optional): inline `--` comment lines preceding a column definition when `IncludeCreateTableFieldComments=true`.

`Constraints` (`*DDLConstraints`) fields:
- `PrimaryKey` (`*DDLPrimaryKey`): `ConstraintName` (optional), `Columns`.
- `ForeignKeys` (`[]DDLForeignKey`): `ConstraintName` (optional), `Columns`, `ReferencesSchema` (optional), `ReferencesTable`, `ReferencesColumns` (optional), `OnDelete` (optional), `OnUpdate` (optional). Referential actions: `CASCADE`, `SET NULL`, `SET DEFAULT`, `RESTRICT`, `NO ACTION`.
- `UniqueKeys` (`[]DDLUniqueConstraint`): `ConstraintName` (optional), `Columns`.
- `CheckConstraints` (`[]DDLCheckConstraint`): `ConstraintName` (optional), `Expression`.

Current DDL convention:
- `CREATE_TABLE` populates `ColumnDetails` and `Constraints` for inline and table-level constraints.
- `COMMENT ON ...` populates `DDLActions` with `Type=COMMENT`.
- Other DDL actions currently do not populate `ColumnDetails`.
- `ALTER_TABLE` uses `Columns` and `Flags` for operation-level details.
- `ALTER_TABLE ... ADD CONSTRAINT` populates `Constraints`.

## Parse Options

`ParseOptions` currently supports:

- `IncludeCreateTableFieldComments`:
  - default `false`
  - when `true`, captures inline `--` field comments in `CREATE TABLE` into `DDLActions[].ColumnDetails[].Comment`.

Notes:
- This option only affects inline `--` field comments in `CREATE TABLE`.
- `COMMENT ON ...` statement extraction is always enabled.

## Command-to-Section Expectations

- `SELECT`: read-query shape + relation metadata.
- `INSERT`: relation metadata + DML (`InsertColumns`, `Upsert`, `Returning`).
- `UPDATE`: relation metadata + DML (`SetClauses`, `Where`, `Returning`).
- `DELETE`: relation metadata + DML (`Where`, `Returning`).
- `MERGE`: relation metadata + `Merge`.
- `DDL`: `DDLActions` (+ `Tables` where applicable).
- `UNKNOWN`: minimal envelope only.

### Compact Field Matrix

| Section / Field Group | SELECT | INSERT | UPDATE | DELETE | MERGE | DDL | UNKNOWN |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Core envelope (`Command`, `RawSQL`, `Parameters`) | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Relations (`Tables`, `CTEs`, `Subqueries`) | Yes | Yes | Yes | Yes | Yes | Sometimes | No |
| Read-query shape (`Columns`, `Where`, `GroupBy`, `OrderBy`, `Limit`, `SetOperations`, `ColumnUsage`) | Yes | No | Partial | Partial | Partial | No | No |
| DML shape (`InsertColumns`, `SetClauses`, `Returning`, `Upsert`) | No | Yes | Yes | Partial | No | No | No |
| MERGE payload (`Merge`) | No | No | No | No | Yes | No | No |
| DDL payload (`DDLActions`) | No | No | No | No | No | Yes | No |

Notes:
- "Partial" means only relevant subsets are filled for that command.
- "Sometimes" under DDL relations means `Tables` is populated for actions where a base relation is explicitly parsed (for example `CREATE TABLE`, `ALTER TABLE`, `TRUNCATE`).
- Empty/nil in unrelated sections is expected behavior.

## Practical Guidance

- Use `Command` first, then inspect relevant sections.
- Treat empty slices/nil in unrelated sections as expected behavior.
- For DDL identity, prefer structured values where available:
  - use `Schema` + `ObjectName` for action identity (`ObjectName` is unqualified)
  - use `Tables` when you need normalized relation references
- For predicate analysis, key off `ColumnUsage.Function` rather than scanning the raw SQL or `Context` string. Walk filter usages, branch on `Function.Name`, and consult `Function.Args` for the literal arguments where needed:

  ```go
  for _, u := range result.ColumnUsage {
      if u.UsageType != analysis.SQLUsageTypeFilter || u.Function == nil {
          continue
      }
      switch u.Function.Name {
      case "length", "char_length", "octet_length":
          // length-class predicate on u.Column
      case "lower", "upper":
          // case-folding predicate on u.Column
      case "date_trunc":
          // u.Function.Args[0].Literal carries the unit when literal
      }
  }
  ```

## Suggested Follow-up Issues

1. `ALTER TABLE` delta metadata model
   - Goal: add operation-level `AlterOps` payload instead of overloading `ColumnDetails`.
   - Scope: `ADD COLUMN`, `DROP COLUMN`, `TYPE`, `SET/DROP DEFAULT`, `SET/DROP NOT NULL`, `RENAME COLUMN`.
   - Non-goal: full pre/post reconstruction without schema state.

2. `CREATE TABLE` type coverage expansion
   - Goal: maintain a broad regression matrix covering common PostgreSQL type families.
   - Scope: numerics, text/binary, time/date, JSON/XML, network, geometric, ranges, arrays.
