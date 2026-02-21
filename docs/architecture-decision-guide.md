# Architecture Decision Guide: Core Parser vs Analysis Layer

Two layers, one rule: **core parser extracts from grammar, analysis interprets from IR**.

## The Two Layers

```
SQL text ──> [ Core Parser ] ──> ParsedQuery (IR) ──> [ Analysis ] ──> Consumer DTOs
                  │                                        │
                  │ ANTLR tree walks                       │ IR + optional schema metadata
                  │ No external inputs                     │ Normalizes, resolves, infers
                  │                                        │
                  ├─ entry.go          ┌─ analysis/analysis.go
                  ├─ ir.go             ├─ analysis/types.go
                  ├─ select.go         ├─ analysis/where_conditions.go
                  ├─ dml_*.go          ├─ analysis/join_parser.go
                  ├─ ddl.go            ├─ analysis/combined_extractor.go
                  └─ merge.go          └─ analysis/helpers.go
```

## Where Does It Go?

```
                      ┌──────────────────────┐
                      │    New feature        │
                      └──────────┬───────────┘
                                 │
                    ┌────────────▼────────────┐
                    │ Needs external inputs?  │
                    │ (schema, PK info, etc.) │
                    └─────┬────────────┬──────┘
                      YES │            │ NO
                          │   ┌────────▼────────┐
                          │   │ Walks ANTLR tree │
                          │   │ (gen.*Context)?  │
                          │   └──┬──────────┬───┘
                          │  YES │          │ NO
                          │      │  ┌───────▼────────────┐
                          │      │  │ Interprets/parses  │
                          │      │  │ raw IR strings?    │
                          │      │  └──┬──────────┬──────┘
                          │      │ YES │          │ NO
                          │      │     │  ┌───────▼────────────┐
                          │      │     │  │ New grammar-backed  │
                          │      │     │  │ IR field?           │
                          │      │     │  └──┬──────────┬──────┘
                          │      │     │ YES │          │ NO
                          ▼      │     ▼     │          ▼
                     ┌─────────┐ │ ┌────────┐│   ┌──────────┐
                     │analysis/│ │ │analysis/││   │ analysis/ │
                     └─────────┘ │ └────────┘│   │ (default) │
                          ┌──────▼──┐  ┌─────▼┐  └──────────┘
                          │  core   │  │ core │
                          │ parser  │  │parser│
                          └─────────┘  └──────┘
```

## API Quick Reference

| You want to... | Use | Layer |
|---|---|---|
| Parse SQL to raw IR | `postgresparser.ParseSQL*` | Core |
| Get consumer-ready query DTO | `analysis.AnalyzeSQL*` | Analysis |
| Get structured WHERE constraints | `analysis.ExtractWhereConditions` | Analysis |
| Infer FK-like JOIN relationships | `analysis.ExtractJoinRelationshipsWithSchema` | Analysis |
| One-pass WHERE + JOIN + schema | `analysis.ExtractQueryAnalysisWithSchema` | Analysis |

## What Goes Where — By Example

### Core parser (grammar extraction)

| Feature | Reason |
|---|---|
| RETURNING clause columns | Direct parse tree node |
| Window function PARTITION BY | Grammar-level clause, walks `gen.*Context` |
| `CONCURRENTLY` flag on DDL | Single flag from parse tree |
| MERGE statement support | New statement type, new visitor |

### Analysis (interpretation + external metadata)

| Feature | Reason |
|---|---|
| Structured WHERE conditions | Interprets raw IR strings (operator, value, table) |
| FK relationship detection | Needs `ColumnSchema.IsPrimaryKey` from caller |
| Query complexity scoring | Derived metric over IR fields |
| Missing-WHERE-on-DELETE check | Semantic rule, not grammar |
| Schema validation | Needs external column metadata |
| `BaseTables()` | Convenience over IR table refs |

## Boundary Rules

1. **Core parser owns grammar extraction.** Value comes from ANTLR nodes/tokens → core IR.
2. **Analysis owns interpretation.** Derived/normalized from IR fields (alias resolution, operator normalization, value parsing) → analysis.
3. **No mirrored contracts.** Don't add the same DTO to both `ir.go` and `analysis/types.go`.
4. **Pay-for-what-you-use.** Heavy interpretation stays behind analysis APIs. `ParseSQL` stays fast for callers who only need IR.

## Case Study: Why `WhereCondition` Lives in Analysis

Not in core IR. Here's why:

1. Core already exposes raw WHERE data (`ParsedQuery.Where`, `ParsedQuery.ColumnUsage`).
2. `WhereCondition` is an interpreted projection — operator normalization, value extraction, alias-to-table resolution.
3. This logic is policy. It evolves independently from parse-tree extraction.
4. Keeping it out of core avoids penalizing callers who only need IR.

Same logic applies to `ExtractJoinRelationshipsWithSchema` and `ExtractQueryAnalysisWithSchema` — they accept `map[string][]ColumnSchema` (external metadata the grammar can't provide). Core's contract is "SQL text in, IR out, no side inputs."

## Anti-Patterns

- Adding analysis DTOs to `ir.go` "for convenience"
- Re-implementing the same transformation in both core and analysis
- Putting schema-dependent logic inside core parser visitors
