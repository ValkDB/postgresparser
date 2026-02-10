## Summary

<!-- 1-3 bullet points describing what this PR does -->

## Layer

<!-- Which layer does this change touch? Check all that apply. -->

- [ ] Core parser (`ir.go`, `ddl.go`, `select.go`, `dml_*.go`, `merge.go`, `setops.go`, `entry.go`)
- [ ] Analysis layer (`analysis/`)
- [ ] Docs / examples
- [ ] CI / tooling

## SQL examples

<!-- Show at least one SQL statement this PR affects, with before/after output if applicable. -->

```sql
-- example
```

## Test plan

<!-- How was this tested? Check all that apply. -->

- [ ] New unit tests added
- [ ] Existing tests updated
- [ ] `make test` passes (race detector + coverage)
- [ ] `make vet` passes
- [ ] Manual verification with `examples/`

## Related issues

<!-- Link related issues: Fixes #123, Part of #456 -->

## Checklist

- [ ] No changes to `gen/` (auto-generated ANTLR code)
- [ ] New IR fields documented in `docs/parsed-query.md` (if applicable)
- [ ] Public API additions are backward compatible
