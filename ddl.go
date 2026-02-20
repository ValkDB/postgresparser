// ddl.go implements DDL population logic for CREATE TABLE, DROP, ALTER TABLE, CREATE INDEX, and TRUNCATE.
package postgresparser

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/valkdb/postgresparser/gen"
)

// populateCreateTable handles CREATE TABLE metadata extraction (table + columns)
// and enforces table-level PRIMARY KEY columns as Nullable=false.
func populateCreateTable(result *ParsedQuery, ctx gen.ICreatestmtContext, tokens antlr.TokenStream, opts ParseOptions) error {
	if ctx == nil {
		return fmt.Errorf("create table statement: %w", ErrNilContext)
	}
	// This rule is specific to CREATE TABLE.
	if ctx.TABLE() == nil {
		return nil
	}

	tableRaw := ""
	if qualified := ctx.Qualified_name(0); qualified != nil {
		if prc, ok := qualified.(antlr.ParserRuleContext); ok {
			tableRaw = strings.TrimSpace(ctxText(tokens, prc))
		}
	}
	schema, tableName := splitQualifiedName(tableRaw)
	if tableRaw != "" {
		result.Tables = append(result.Tables, TableRef{
			Schema: schema,
			Name:   tableName,
			Type:   TableTypeBase,
			Raw:    tableRaw,
		})
	}

	var flags []string
	if ctx.IF_P() != nil && ctx.NOT() != nil && ctx.EXISTS() != nil {
		flags = append(flags, "IF_NOT_EXISTS")
	}

	action := DDLAction{
		Type:       DDLCreateTable,
		ObjectName: tableName,
		Schema:     schema,
		Flags:      flags,
	}

	if optElems := ctx.Opttableelementlist(); optElems != nil && optElems.Tableelementlist() != nil {
		tableElems := optElems.Tableelementlist().AllTableelement()
		action.Columns = make([]string, 0, len(tableElems))
		action.ColumnDetails = make([]DDLColumn, 0, len(tableElems))
		primaryKeyCols := collectCreateTablePrimaryKeyColumns(tableElems)
		for _, tableElem := range tableElems {
			if tableElem == nil || tableElem.ColumnDef() == nil {
				continue
			}
			col := extractCreateTableColumn(tableElem.ColumnDef(), tokens, opts.IncludeCreateTableFieldComments)
			if col.Name == "" {
				continue
			}
			if _, ok := primaryKeyCols[normalizeCreateTableColumnName(col.Name)]; ok {
				// A table-level PRIMARY KEY also implies NOT NULL.
				col.Nullable = false
			}
			action.Columns = append(action.Columns, col.Name)
			action.ColumnDetails = append(action.ColumnDetails, col)
		}
	}

	result.DDLActions = append(result.DDLActions, action)
	return nil
}

// extractCreateTableColumn extracts metadata for a single CREATE TABLE column definition.
func extractCreateTableColumn(colDef gen.IColumnDefContext, tokens antlr.TokenStream, includeComments bool) DDLColumn {
	if colDef == nil {
		return DDLColumn{}
	}

	var col DDLColumn
	if colid := colDef.Colid(); colid != nil {
		if prc, ok := colid.(antlr.ParserRuleContext); ok {
			col.Name = strings.TrimSpace(ctxText(tokens, prc))
		}
	}
	if includeComments {
		col.Comment = extractColumnLeadingLineComments(colDef, tokens)
	}
	if typ := colDef.Typename(); typ != nil {
		if prc, ok := typ.(antlr.ParserRuleContext); ok {
			col.Type = normalizeSpace(ctxText(tokens, prc))
		}
	}

	col.Nullable = true // PostgreSQL defaults to nullable unless constrained.
	if quals := colDef.Colquallist(); quals != nil {
		for _, constraint := range quals.AllColconstraint() {
			if constraint == nil || constraint.Colconstraintelem() == nil {
				continue
			}
			elem := constraint.Colconstraintelem()

			// PRIMARY KEY implies NOT NULL in PostgreSQL.
			if (elem.NOT() != nil && elem.NULL_P() != nil) || (elem.PRIMARY() != nil && elem.KEY() != nil) {
				col.Nullable = false
			}

			if elem.DEFAULT() != nil && elem.B_expr() != nil {
				if prc, ok := elem.B_expr().(antlr.ParserRuleContext); ok {
					col.Default = strings.TrimSpace(ctxText(tokens, prc))
				}
			}
		}
	}
	return col
}

// collectCreateTablePrimaryKeyColumns extracts column names referenced by table-level PRIMARY KEY constraints.
func collectCreateTablePrimaryKeyColumns(tableElems []gen.ITableelementContext) map[string]struct{} {
	pkCols := make(map[string]struct{}, len(tableElems))
	for _, tableElem := range tableElems {
		if tableElem == nil || tableElem.Tableconstraint() == nil {
			continue
		}
		constraint := tableElem.Tableconstraint().Constraintelem()
		if constraint == nil || constraint.PRIMARY() == nil || constraint.KEY() == nil || constraint.Columnlist() == nil {
			continue
		}
		for _, colElem := range constraint.Columnlist().AllColumnElem() {
			pkCols[normalizeCreateTableColumnName(colElem.Colid().GetText())] = struct{}{}
		}
	}
	return pkCols
}

// normalizeCreateTableColumnName keeps PostgreSQL identifier semantics for matching:
// quoted identifiers keep case, while unquoted identifiers are case-insensitive.
func normalizeCreateTableColumnName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	if len(trimmed) >= 2 && strings.HasPrefix(trimmed, `"`) && strings.HasSuffix(trimmed, `"`) {
		return strings.ReplaceAll(trimmed[1:len(trimmed)-1], `""`, `"`)
	}
	return strings.ToLower(trimIdentQuotes(trimmed))
}

// populateDropStmt handles DROP TABLE, DROP INDEX, and DROP INDEX CONCURRENTLY.
func populateDropStmt(result *ParsedQuery, ctx gen.IDropstmtContext, tokens antlr.TokenStream) error {
	if ctx == nil {
		return fmt.Errorf("drop statement: %w", ErrNilContext)
	}

	// Determine shared flags.
	var flags []string
	ifExists := ctx.IF_P() != nil && ctx.EXISTS() != nil
	if ifExists {
		flags = append(flags, "IF_EXISTS")
	}
	if db := ctx.Drop_behavior_(); db != nil {
		if db.CASCADE() != nil {
			flags = append(flags, "CASCADE")
		} else if db.RESTRICT() != nil {
			flags = append(flags, "RESTRICT")
		}
	}

	// DROP INDEX CONCURRENTLY (special grammar alternatives).
	if ctx.CONCURRENTLY() != nil {
		flags = append(flags, "CONCURRENTLY")
		if nameList := ctx.Any_name_list_(); nameList != nil {
			for _, anyName := range nameList.AllAny_name() {
				prc, ok := anyName.(antlr.ParserRuleContext)
				if !ok {
					continue
				}
				name := strings.TrimSpace(ctxText(tokens, prc))
				schema, objectName := splitQualifiedName(name)
				result.DDLActions = append(result.DDLActions, DDLAction{
					Type:       DDLDropIndex,
					ObjectName: objectName,
					Schema:     schema,
					Flags:      copyFlags(flags),
				})
			}
		}
		return nil
	}

	// DROP object_type_any_name ... (TABLE, INDEX, VIEW, etc.)
	if objType := ctx.Object_type_any_name(); objType != nil {
		if nameList := ctx.Any_name_list_(); nameList != nil {
			switch {
			case objType.TABLE() != nil:
				for _, anyName := range nameList.AllAny_name() {
					prc, ok := anyName.(antlr.ParserRuleContext)
					if !ok {
						continue
					}
					nameText := strings.TrimSpace(ctxText(tokens, prc))
					schema, tableName := splitQualifiedName(nameText)
					result.DDLActions = append(result.DDLActions, DDLAction{
						Type:       DDLDropTable,
						ObjectName: tableName,
						Schema:     schema,
						Flags:      copyFlags(flags),
					})
					result.Tables = append(result.Tables, TableRef{
						Schema: schema,
						Name:   tableName,
						Type:   TableTypeBase,
						Raw:    nameText,
					})
				}
			case objType.INDEX() != nil:
				for _, anyName := range nameList.AllAny_name() {
					prc, ok := anyName.(antlr.ParserRuleContext)
					if !ok {
						continue
					}
					name := strings.TrimSpace(ctxText(tokens, prc))
					schema, objectName := splitQualifiedName(name)
					result.DDLActions = append(result.DDLActions, DDLAction{
						Type:       DDLDropIndex,
						ObjectName: objectName,
						Schema:     schema,
						Flags:      copyFlags(flags),
					})
				}
			}
		}
	}
	return nil
}

// populateAlterTable handles ALTER TABLE with ADD/DROP/ALTER column sub-commands.
func populateAlterTable(result *ParsedQuery, ctx gen.IAltertablestmtContext, tokens antlr.TokenStream) error {
	if ctx == nil {
		return fmt.Errorf("alter table statement: %w", ErrNilContext)
	}
	// Only handle ALTER TABLE (not ALTER INDEX/VIEW/SEQUENCE).
	if ctx.TABLE() == nil {
		return nil
	}

	tableRaw := ""
	tableName := ""
	tableSchema := ""
	if rel := ctx.Relation_expr(); rel != nil {
		tableRaw, tableSchema, tableName = extractRelationExprNameParts(rel, tokens)
		result.Tables = append(result.Tables, TableRef{
			Schema: tableSchema,
			Name:   tableName,
			Type:   TableTypeBase,
			Raw:    tableRaw,
		})
	}

	cmds := ctx.Alter_table_cmds()
	if cmds == nil {
		return nil
	}
	for _, cmd := range cmds.AllAlter_table_cmd() {
		populateAlterTableCmd(result, cmd, tokens, tableName, tableSchema)
	}
	return nil
}

// populateAlterTableCmd processes a single ALTER TABLE sub-command.
func populateAlterTableCmd(result *ParsedQuery, cmd gen.IAlter_table_cmdContext, tokens antlr.TokenStream, tableName, tableSchema string) {
	if cmd == nil {
		return
	}

	var flags []string
	if db := cmd.Drop_behavior_(); db != nil {
		if db.CASCADE() != nil {
			flags = append(flags, "CASCADE")
		} else if db.RESTRICT() != nil {
			flags = append(flags, "RESTRICT")
		}
	}

	switch {
	case cmd.DROP() != nil:
		// DROP COLUMN vs DROP CONSTRAINT
		if cmd.CONSTRAINT() != nil {
			// Skip constraint drops — not column-level DDL.
			return
		}
		colName := extractAlterCmdColumnName(cmd, tokens)
		if colName == "" {
			return
		}
		if cmd.IF_P() != nil && cmd.EXISTS() != nil {
			flags = append(flags, "IF_EXISTS")
		}
		result.DDLActions = append(result.DDLActions, DDLAction{
			Type:       DDLDropColumn,
			ObjectName: tableName,
			Schema:     tableSchema,
			Columns:    []string{colName},
			Flags:      flags,
		})

	case cmd.ADD_P() != nil:
		if cmd.CONSTRAINT() != nil || cmd.Tableconstraint() != nil {
			// Skip ADD CONSTRAINT.
			return
		}
		colName := ""
		if colDef := cmd.ColumnDef(); colDef != nil {
			if colDef.Colid() != nil {
				if prc, ok := colDef.Colid().(antlr.ParserRuleContext); ok {
					colName = strings.TrimSpace(ctxText(tokens, prc))
				}
			}
		}
		if colName == "" {
			return
		}
		addFlags := copyFlags(flags)
		addFlags = append(addFlags, "ADD_COLUMN")
		if cmd.IF_P() != nil && cmd.NOT() != nil && cmd.EXISTS() != nil {
			addFlags = append(addFlags, "IF_NOT_EXISTS")
		}
		result.DDLActions = append(result.DDLActions, DDLAction{
			Type:       DDLAlterTable,
			ObjectName: tableName,
			Schema:     tableSchema,
			Columns:    []string{colName},
			Flags:      addFlags,
		})

	case cmd.ALTER() != nil:
		colName := extractAlterCmdColumnName(cmd, tokens)
		if colName == "" {
			return
		}
		alterFlags := copyFlags(flags)
		alterFlags = append(alterFlags, "ALTER_COLUMN")
		result.DDLActions = append(result.DDLActions, DDLAction{
			Type:       DDLAlterTable,
			ObjectName: tableName,
			Schema:     tableSchema,
			Columns:    []string{colName},
			Flags:      alterFlags,
		})

	default:
		// Other sub-commands (OWNER TO, SET, etc.) — generic ALTER_TABLE action.
		result.DDLActions = append(result.DDLActions, DDLAction{
			Type:       DDLAlterTable,
			ObjectName: tableName,
			Schema:     tableSchema,
			Flags:      flags,
		})
	}
}

// extractAlterCmdColumnName extracts the column name from an ALTER TABLE sub-command.
func extractAlterCmdColumnName(cmd gen.IAlter_table_cmdContext, tokens antlr.TokenStream) string {
	// The column name is usually the first Colid child.
	colids := cmd.AllColid()
	if len(colids) > 0 {
		if prc, ok := colids[0].(antlr.ParserRuleContext); ok {
			return strings.TrimSpace(ctxText(tokens, prc))
		}
	}
	return ""
}

// extractRelationExprNameParts extracts relation expression text and normalized schema/name.
// It prefers the structured Qualified_name() AST node so modifiers like ONLY do not leak into schema.
func extractRelationExprNameParts(rel gen.IRelation_exprContext, tokens antlr.TokenStream) (raw, schema, name string) {
	if rel == nil {
		return "", "", ""
	}
	if prc, ok := rel.(antlr.ParserRuleContext); ok {
		raw = strings.TrimSpace(ctxText(tokens, prc))
	}

	if qualified := rel.Qualified_name(); qualified != nil {
		if prc, ok := qualified.(antlr.ParserRuleContext); ok {
			schema, name = splitQualifiedName(strings.TrimSpace(ctxText(tokens, prc)))
			return raw, schema, name
		}
	}
	schema, name = splitQualifiedName(raw)
	return raw, schema, name
}

// populateCreateIndex handles CREATE [UNIQUE] INDEX [CONCURRENTLY] ... ON table.
func populateCreateIndex(result *ParsedQuery, ctx gen.IIndexstmtContext, tokens antlr.TokenStream) error {
	if ctx == nil {
		return fmt.Errorf("create index statement: %w", ErrNilContext)
	}

	indexRaw := ""
	if idx := ctx.Index_name_(); idx != nil {
		if prc, ok := idx.(antlr.ParserRuleContext); ok {
			indexRaw = strings.TrimSpace(ctxText(tokens, prc))
		}
	}

	tableRaw := ""
	tableName := ""
	tableSchema := ""
	if rel := ctx.Relation_expr(); rel != nil {
		tableRaw, tableSchema, tableName = extractRelationExprNameParts(rel, tokens)
		result.Tables = append(result.Tables, TableRef{
			Schema: tableSchema,
			Name:   tableName,
			Type:   TableTypeBase,
			Raw:    tableRaw,
		})
	}

	var columns []string
	if params := ctx.Index_params(); params != nil {
		for _, elem := range params.AllIndex_elem() {
			prc, ok := elem.(antlr.ParserRuleContext)
			if !ok {
				continue
			}
			text := strings.TrimSpace(ctxText(tokens, prc))
			if text != "" {
				columns = append(columns, text)
			}
		}
	}

	var flags []string
	if ctx.Concurrently_() != nil {
		flags = append(flags, "CONCURRENTLY")
	}
	if ctx.Unique_() != nil {
		flags = append(flags, "UNIQUE")
	}
	if ctx.IF_P() != nil && ctx.NOT() != nil && ctx.EXISTS() != nil {
		flags = append(flags, "IF_NOT_EXISTS")
	}

	indexType := ""
	if amc := ctx.Access_method_clause(); amc != nil {
		if amc.Name() != nil {
			if prc, ok := amc.Name().(antlr.ParserRuleContext); ok {
				indexType = strings.TrimSpace(ctxText(tokens, prc))
			}
		}
	}

	indexSchema, indexName := splitQualifiedName(indexRaw)
	if indexSchema == "" {
		indexSchema = tableSchema
	}

	action := DDLAction{
		Type:       DDLCreateIndex,
		ObjectName: indexName,
		Schema:     indexSchema,
		Columns:    columns,
		Flags:      flags,
		IndexType:  indexType,
	}
	result.DDLActions = append(result.DDLActions, action)
	return nil
}

// populateTruncate handles TRUNCATE [TABLE] table [, ...] [CASCADE|RESTRICT].
func populateTruncate(result *ParsedQuery, ctx gen.ITruncatestmtContext, tokens antlr.TokenStream) error {
	if ctx == nil {
		return fmt.Errorf("truncate statement: %w", ErrNilContext)
	}

	var flags []string
	if rs := ctx.Restart_seqs_(); rs != nil {
		if rs.RESTART() != nil {
			flags = append(flags, "RESTART_IDENTITY")
		} else if rs.CONTINUE_P() != nil {
			flags = append(flags, "CONTINUE_IDENTITY")
		}
	}
	if db := ctx.Drop_behavior_(); db != nil {
		if db.CASCADE() != nil {
			flags = append(flags, "CASCADE")
		} else if db.RESTRICT() != nil {
			flags = append(flags, "RESTRICT")
		}
	}

	if relList := ctx.Relation_expr_list(); relList != nil {
		for _, rel := range relList.AllRelation_expr() {
			raw, schema, name := extractRelationExprNameParts(rel, tokens)
			result.DDLActions = append(result.DDLActions, DDLAction{
				Type:       DDLTruncate,
				ObjectName: name,
				Schema:     schema,
				Flags:      copyFlags(flags),
			})
			result.Tables = append(result.Tables, TableRef{
				Schema: schema,
				Name:   name,
				Type:   TableTypeBase,
				Raw:    raw,
			})
		}
	}
	return nil
}

// copyFlags returns a copy of the flags slice to avoid shared backing arrays.
func copyFlags(flags []string) []string {
	if len(flags) == 0 {
		return nil
	}
	out := make([]string, len(flags))
	copy(out, flags)
	return out
}

// normalizeSpace collapses repeated internal whitespace to a single space.
func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
}
