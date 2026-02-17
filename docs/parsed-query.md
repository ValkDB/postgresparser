# ParsedQuery IR Reference

This document defines how to interpret parser outputs:
- `postgresparser.ParseSQL` / `postgresparser.ParseSQLStrict` output (`ParsedQuery` in `ir.go`)
- `postgresparser.ParseSQLAll` output (`ParseBatchResult` in `ir.go`)

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
- Unrelated sections are expected to be empty for a given command.
- `Command` is the primary discriminator for which sections to read.

## ParseBatchResult

`ParseBatchResult` fields:
- `Statements`: One `StatementParseResult` per input statement in source order.
  - `Index`: 1-based input statement index.
  - `RawSQL`: statement-scoped SQL text.
  - `Query`: parsed IR when statement conversion succeeds (`nil` on failure).
  - `Warnings`: statement-scoped warnings (`SYNTAX_ERROR`).
- `TotalStatements`: Number of statements detected in input.
- `ParsedStatements`: Number of statements with non-`nil` `Query`.
- `HasFailures`: `true` when any statement has a nil `Query` or any `Warnings`.

## Core Envelope

- `Command`: High-level statement type (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DDL`, `UNKNOWN`).
- `RawSQL`: Preprocessed SQL string used for parsing.
- `Parameters`: Positional/anonymous parameter placeholders (`$1`, `?`, etc.).

## Relation Metadata

- `Tables`: Structured relation refs (`Schema`, `Name`, `Alias`, `Type`, `Raw`).
- `CTEs`: `WITH` definitions.
- `Subqueries`: Nested query refs discovered in the statement.
- `JoinConditions`: Raw join condition expressions.
- `Correlations`: Outer/inner alias correlation metadata for lateral/correlated subqueries.

## Read-Query Shape

- `Columns`: Projection expressions and aliases.
- `ColumnUsage`: Expression-level column usage classification.
- `Where`: WHERE/CURRENT clauses as raw expressions.
- `Having`: HAVING clauses.
- `GroupBy`: GROUP BY expressions.
- `OrderBy`: ORDER BY expressions + direction/nulls modifiers.
- `Limit`: LIMIT/OFFSET metadata.
- `SetOperations`: UNION/INTERSECT/EXCEPT branches.
- `DerivedColumns`: Alias-to-expression map for derived projection columns.

## DML Shape

- `InsertColumns`: Target columns for INSERT.
- `SetClauses`: SET clauses for UPDATE (and related clause extraction).
- `Returning`: RETURNING clauses.
- `Upsert`: `ON CONFLICT` metadata for INSERT.
- `Merge`: MERGE metadata (target/source/condition/actions).

## DDL Shape

- `DDLActions`: Normalized DDL actions extracted from DDL statements.

Common DDL action fields:
- `Type`: `CREATE_TABLE`, `DROP_TABLE`, `DROP_COLUMN`, `ALTER_TABLE`, `CREATE_INDEX`, `DROP_INDEX`, `TRUNCATE`.
- `ObjectName`: Unqualified target object identifier.
- `Schema`: Parsed schema when available.
- `Columns`: Column names or indexed expressions relevant to the action.
- `Flags`: Modifiers like `IF_EXISTS`, `IF_NOT_EXISTS`, `CASCADE`, `CONCURRENTLY`, etc.
- `IndexType`: Index method for `CREATE_INDEX` (for example `btree`, `gin`).
- `ColumnDetails`: Column metadata for `CREATE_TABLE` actions.

`ColumnDetails` (`[]DDLColumn`) fields:
- `Name`
- `Type`
- `Nullable`
- `Default`

Current DDL convention:
- `CREATE_TABLE` populates `ColumnDetails`.
- Other DDL actions currently do not populate `ColumnDetails`.
- `ALTER_TABLE` uses `Columns` and `Flags` for operation-level details.

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

## Suggested Follow-up Issues

1. `ALTER TABLE` delta metadata model
   - Goal: add operation-level `AlterOps` payload instead of overloading `ColumnDetails`.
   - Scope: `ADD COLUMN`, `DROP COLUMN`, `TYPE`, `SET/DROP DEFAULT`, `SET/DROP NOT NULL`, `RENAME COLUMN`.
   - Non-goal: full pre/post reconstruction without schema state.

2. `CREATE TABLE` type coverage expansion
   - Goal: maintain a broad regression matrix covering common PostgreSQL type families.
   - Scope: numerics, text/binary, time/date, JSON/XML, network, geometric, ranges, arrays.
