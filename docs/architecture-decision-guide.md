# Architecture Decision Guide: Core Parser vs Analysis Layer

Where does a new feature belong? Use this guide when deciding.

- **Core parser** (`postgresparser` root) — SQL text in, `ParsedQuery` IR out. Walks ANTLR parse tree nodes. No external inputs.
  Key files: `entry.go`, `ir.go`, `select.go`, `dml_*.go`, `ddl.go`, `merge.go`, `setops.go`

- **Analysis layer** (`analysis/`) — operates on `*ParsedQuery` + optional external metadata (`ColumnSchema`). Interprets, composes, enriches.
  Key files: `analysis/analysis.go`, `analysis/types.go`, `analysis/where_conditions.go`, `analysis/join_parser.go`, `analysis/combined_extractor.go`, `analysis/helpers.go`

## Decision Flowchart

```
New feature or enhancement
|
+- Does it require external inputs beyond SQL text?
|  (schema metadata, PK info, config, runtime state)
|  YES -> analysis/
|
+- Does it parse/interpret raw string fields from the IR?
|  (extracting operators from WHERE strings, splitting JOIN conditions)
|  YES -> analysis/
|
+- Does it walk ANTLR parse tree nodes (gen.*Context)?
|  YES -> core parser (ddl.go, select.go, dml_*.go, or new file)
|
+- Is it a new IR field that the grammar directly provides?
|  YES -> core parser (ir.go for type, visitor file for extraction)
|
+- Is it a convenience/utility over existing IR fields?
|  YES -> analysis/helpers.go
|
+- None of the above?
   -> Discuss. It's probably analysis/, but worth reviewing.
```

## Examples

### Core parser:

| Feature | Why |
|---|---|
| Extract RETURNING clause columns | Direct parse tree extraction |
| Parse window function PARTITION BY | Grammar-level clause, walks `gen.*Context` |
| Add `CONCURRENTLY` flag to DDL | Direct flag from parse tree node |
| Support MERGE statement | New statement type, needs new visitor file |

### Analysis:

| Feature | Why |
|---|---|
| Structured WHERE conditions (operator, value, table) | Interprets raw IR strings |
| FK relationship detection from JOINs | Requires schema metadata (`IsPrimaryKey`) |
| Query complexity scoring | Derived metric from IR fields |
| Missing-WHERE-on-DELETE detection | Semantic rule, not grammar extraction |
| Schema validation (do referenced columns exist?) | Requires external schema metadata |
| `BaseTables()` | Convenience function over IR/DTO |

## Why WithSchema Methods Stay in Analysis

`ExtractJoinRelationshipsWithSchema` and `ExtractQueryAnalysisWithSchema` accept `map[string][]ColumnSchema` — external metadata the grammar cannot provide. The core parser's contract is "SQL text in, IR out" with no side inputs. These functions require DB metadata, perform semantic inference, and return types with no grammar-level equivalent.
