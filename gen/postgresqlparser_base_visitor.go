// Code generated from PostgreSQLParser.g4 by ANTLR 4.13.1. DO NOT EDIT.

package gen // PostgreSQLParser
import "github.com/antlr4-go/antlr/v4"

type BasePostgreSQLParserVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BasePostgreSQLParserVisitor) VisitRoot(ctx *RootContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitStmtblock(ctx *StmtblockContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitStmtmulti(ctx *StmtmultiContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitStmt(ctx *StmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCallstmt(ctx *CallstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreaterolestmt(ctx *CreaterolestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWith_(ctx *With_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptrolelist(ctx *OptrolelistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlteroptrolelist(ctx *AlteroptrolelistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlteroptroleelem(ctx *AlteroptroleelemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateoptroleelem(ctx *CreateoptroleelemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateuserstmt(ctx *CreateuserstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterrolestmt(ctx *AlterrolestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIn_database_(ctx *In_database_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterrolesetstmt(ctx *AlterrolesetstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDroprolestmt(ctx *DroprolestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreategroupstmt(ctx *CreategroupstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltergroupstmt(ctx *AltergroupstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAdd_drop(ctx *Add_dropContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateschemastmt(ctx *CreateschemastmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptschemaname(ctx *OptschemanameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptschemaeltlist(ctx *OptschemaeltlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSchema_stmt(ctx *Schema_stmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVariablesetstmt(ctx *VariablesetstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSet_rest(ctx *Set_restContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGeneric_set(ctx *Generic_setContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSet_rest_more(ctx *Set_rest_moreContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVar_name(ctx *Var_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVar_list(ctx *Var_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVar_value(ctx *Var_valueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIso_level(ctx *Iso_levelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBoolean_or_string_(ctx *Boolean_or_string_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitZone_value(ctx *Zone_valueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEncoding_(ctx *Encoding_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNonreservedword_or_sconst(ctx *Nonreservedword_or_sconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVariableresetstmt(ctx *VariableresetstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReset_rest(ctx *Reset_restContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGeneric_reset(ctx *Generic_resetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSetresetclause(ctx *SetresetclauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunctionsetresetclause(ctx *FunctionsetresetclauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVariableshowstmt(ctx *VariableshowstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstraintssetstmt(ctx *ConstraintssetstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstraints_set_list(ctx *Constraints_set_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstraints_set_mode(ctx *Constraints_set_modeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCheckpointstmt(ctx *CheckpointstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDiscardstmt(ctx *DiscardstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltertablestmt(ctx *AltertablestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_table_cmds(ctx *Alter_table_cmdsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPartition_cmd(ctx *Partition_cmdContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndex_partition_cmd(ctx *Index_partition_cmdContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_table_cmd(ctx *Alter_table_cmdContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_column_default(ctx *Alter_column_defaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDrop_behavior_(ctx *Drop_behavior_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCollate_clause_(ctx *Collate_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_using(ctx *Alter_usingContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReplica_identity(ctx *Replica_identityContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReloptions(ctx *ReloptionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReloptions_(ctx *Reloptions_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReloption_list(ctx *Reloption_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReloption_elem(ctx *Reloption_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_identity_column_option_list(ctx *Alter_identity_column_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_identity_column_option(ctx *Alter_identity_column_optionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPartitionboundspec(ctx *PartitionboundspecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitHash_partbound_elem(ctx *Hash_partbound_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitHash_partbound(ctx *Hash_partboundContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltercompositetypestmt(ctx *AltercompositetypestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_type_cmds(ctx *Alter_type_cmdsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_type_cmd(ctx *Alter_type_cmdContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCloseportalstmt(ctx *CloseportalstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopystmt(ctx *CopystmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_from(ctx *Copy_fromContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitProgram_(ctx *Program_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_file_name(ctx *Copy_file_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_options(ctx *Copy_optionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_opt_list(ctx *Copy_opt_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_opt_item(ctx *Copy_opt_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBinary_(ctx *Binary_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_delimiter(ctx *Copy_delimiterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUsing_(ctx *Using_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_generic_opt_list(ctx *Copy_generic_opt_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_generic_opt_elem(ctx *Copy_generic_opt_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_generic_opt_arg(ctx *Copy_generic_opt_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_generic_opt_arg_list(ctx *Copy_generic_opt_arg_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCopy_generic_opt_arg_list_item(ctx *Copy_generic_opt_arg_list_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatestmt(ctx *CreatestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpttemp(ctx *OpttempContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpttableelementlist(ctx *OpttableelementlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpttypedtableelementlist(ctx *OpttypedtableelementlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTableelementlist(ctx *TableelementlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTypedtableelementlist(ctx *TypedtableelementlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTableelement(ctx *TableelementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTypedtableelement(ctx *TypedtableelementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColumnDef(ctx *ColumnDefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColumnOptions(ctx *ColumnOptionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColquallist(ctx *ColquallistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColconstraint(ctx *ColconstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColconstraintelem(ctx *ColconstraintelemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGenerated_when(ctx *Generated_whenContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstraintattr(ctx *ConstraintattrContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTablelikeclause(ctx *TablelikeclauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTablelikeoptionlist(ctx *TablelikeoptionlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTablelikeoption(ctx *TablelikeoptionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTableconstraint(ctx *TableconstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstraintelem(ctx *ConstraintelemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNo_inherit_(ctx *No_inherit_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColumn_list_(ctx *Column_list_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColumnlist(ctx *ColumnlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColumnElem(ctx *ColumnElemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitC_include_(ctx *C_include_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitKey_match(ctx *Key_matchContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExclusionconstraintlist(ctx *ExclusionconstraintlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExclusionconstraintelem(ctx *ExclusionconstraintelemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExclusionwhereclause(ctx *ExclusionwhereclauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitKey_actions(ctx *Key_actionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitKey_update(ctx *Key_updateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitKey_delete(ctx *Key_deleteContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitKey_action(ctx *Key_actionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptinherit(ctx *OptinheritContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptpartitionspec(ctx *OptpartitionspecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPartitionspec(ctx *PartitionspecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPart_params(ctx *Part_paramsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPart_elem(ctx *Part_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTable_access_method_clause(ctx *Table_access_method_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptwith(ctx *OptwithContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOncommitoption(ctx *OncommitoptionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpttablespace(ctx *OpttablespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptconstablespace(ctx *OptconstablespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExistingindex(ctx *ExistingindexContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatestatsstmt(ctx *CreatestatsstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterstatsstmt(ctx *AlterstatsstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateasstmt(ctx *CreateasstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreate_as_target(ctx *Create_as_targetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWith_data_(ctx *With_data_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatematviewstmt(ctx *CreatematviewstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreate_mv_target(ctx *Create_mv_targetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptnolog(ctx *OptnologContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRefreshmatviewstmt(ctx *RefreshmatviewstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateseqstmt(ctx *CreateseqstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterseqstmt(ctx *AlterseqstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptseqoptlist(ctx *OptseqoptlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptparenthesizedseqoptlist(ctx *OptparenthesizedseqoptlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSeqoptlist(ctx *SeqoptlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSeqoptelem(ctx *SeqoptelemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBy_(ctx *By_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNumericonly(ctx *NumericonlyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNumericonly_list(ctx *Numericonly_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateplangstmt(ctx *CreateplangstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTrusted_(ctx *Trusted_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitHandler_name(ctx *Handler_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInline_handler_(ctx *Inline_handler_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitValidator_clause(ctx *Validator_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitValidator_(ctx *Validator_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitProcedural_(ctx *Procedural_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatetablespacestmt(ctx *CreatetablespacestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpttablespaceowner(ctx *OpttablespaceownerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDroptablespacestmt(ctx *DroptablespacestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateextensionstmt(ctx *CreateextensionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreate_extension_opt_list(ctx *Create_extension_opt_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreate_extension_opt_item(ctx *Create_extension_opt_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterextensionstmt(ctx *AlterextensionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_extension_opt_list(ctx *Alter_extension_opt_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_extension_opt_item(ctx *Alter_extension_opt_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterextensioncontentsstmt(ctx *AlterextensioncontentsstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatefdwstmt(ctx *CreatefdwstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFdw_option(ctx *Fdw_optionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFdw_options(ctx *Fdw_optionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFdw_options_(ctx *Fdw_options_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterfdwstmt(ctx *AlterfdwstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreate_generic_options(ctx *Create_generic_optionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGeneric_option_list(ctx *Generic_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_generic_options(ctx *Alter_generic_optionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_generic_option_list(ctx *Alter_generic_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlter_generic_option_elem(ctx *Alter_generic_option_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGeneric_option_elem(ctx *Generic_option_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGeneric_option_name(ctx *Generic_option_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGeneric_option_arg(ctx *Generic_option_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateforeignserverstmt(ctx *CreateforeignserverstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitType_(ctx *Type_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitForeign_server_version(ctx *Foreign_server_versionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitForeign_server_version_(ctx *Foreign_server_version_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterforeignserverstmt(ctx *AlterforeignserverstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateforeigntablestmt(ctx *CreateforeigntablestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitImportforeignschemastmt(ctx *ImportforeignschemastmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitImport_qualification_type(ctx *Import_qualification_typeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitImport_qualification(ctx *Import_qualificationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateusermappingstmt(ctx *CreateusermappingstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAuth_ident(ctx *Auth_identContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDropusermappingstmt(ctx *DropusermappingstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterusermappingstmt(ctx *AlterusermappingstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatepolicystmt(ctx *CreatepolicystmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterpolicystmt(ctx *AlterpolicystmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRowsecurityoptionalexpr(ctx *RowsecurityoptionalexprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRowsecurityoptionalwithcheck(ctx *RowsecurityoptionalwithcheckContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRowsecuritydefaulttorole(ctx *RowsecuritydefaulttoroleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRowsecurityoptionaltorole(ctx *RowsecurityoptionaltoroleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRowsecuritydefaultpermissive(ctx *RowsecuritydefaultpermissiveContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRowsecuritydefaultforcmd(ctx *RowsecuritydefaultforcmdContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRow_security_cmd(ctx *Row_security_cmdContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateamstmt(ctx *CreateamstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAm_type(ctx *Am_typeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatetrigstmt(ctx *CreatetrigstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggeractiontime(ctx *TriggeractiontimeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggerevents(ctx *TriggereventsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggeroneevent(ctx *TriggeroneeventContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggerreferencing(ctx *TriggerreferencingContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggertransitions(ctx *TriggertransitionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggertransition(ctx *TriggertransitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransitionoldornew(ctx *TransitionoldornewContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransitionrowortable(ctx *TransitionrowortableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransitionrelname(ctx *TransitionrelnameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggerforspec(ctx *TriggerforspecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggerforopteach(ctx *TriggerforopteachContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggerfortype(ctx *TriggerfortypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggerwhen(ctx *TriggerwhenContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunction_or_procedure(ctx *Function_or_procedureContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggerfuncargs(ctx *TriggerfuncargsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTriggerfuncarg(ctx *TriggerfuncargContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOptconstrfromtable(ctx *OptconstrfromtableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstraintattributespec(ctx *ConstraintattributespecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstraintattributeElem(ctx *ConstraintattributeElemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateeventtrigstmt(ctx *CreateeventtrigstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEvent_trigger_when_list(ctx *Event_trigger_when_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEvent_trigger_when_item(ctx *Event_trigger_when_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEvent_trigger_value_list(ctx *Event_trigger_value_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltereventtrigstmt(ctx *AltereventtrigstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEnable_trigger(ctx *Enable_triggerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateassertionstmt(ctx *CreateassertionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDefinestmt(ctx *DefinestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDefinition(ctx *DefinitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDef_list(ctx *Def_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDef_elem(ctx *Def_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDef_arg(ctx *Def_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOld_aggr_definition(ctx *Old_aggr_definitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOld_aggr_list(ctx *Old_aggr_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOld_aggr_elem(ctx *Old_aggr_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEnum_val_list_(ctx *Enum_val_list_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEnum_val_list(ctx *Enum_val_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterenumstmt(ctx *AlterenumstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIf_not_exists_(ctx *If_not_exists_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateopclassstmt(ctx *CreateopclassstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpclass_item_list(ctx *Opclass_item_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpclass_item(ctx *Opclass_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDefault_(ctx *Default_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpfamily_(ctx *Opfamily_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpclass_purpose(ctx *Opclass_purposeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRecheck_(ctx *Recheck_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateopfamilystmt(ctx *CreateopfamilystmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlteropfamilystmt(ctx *AlteropfamilystmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpclass_drop_list(ctx *Opclass_drop_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpclass_drop(ctx *Opclass_dropContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDropopclassstmt(ctx *DropopclassstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDropopfamilystmt(ctx *DropopfamilystmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDropownedstmt(ctx *DropownedstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReassignownedstmt(ctx *ReassignownedstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDropstmt(ctx *DropstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitObject_type_any_name(ctx *Object_type_any_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitObject_type_name(ctx *Object_type_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDrop_type_name(ctx *Drop_type_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitObject_type_name_on_any_name(ctx *Object_type_name_on_any_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAny_name_list_(ctx *Any_name_list_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAny_name(ctx *Any_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAttrs(ctx *AttrsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitType_name_list(ctx *Type_name_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTruncatestmt(ctx *TruncatestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRestart_seqs_(ctx *Restart_seqs_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCommentstmt(ctx *CommentstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitComment_text(ctx *Comment_textContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSeclabelstmt(ctx *SeclabelstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitProvider_(ctx *Provider_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSecurity_label(ctx *Security_labelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFetchstmt(ctx *FetchstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFetch_args(ctx *Fetch_argsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFrom_in(ctx *From_inContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFrom_in_(ctx *From_in_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGrantstmt(ctx *GrantstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRevokestmt(ctx *RevokestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPrivileges(ctx *PrivilegesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPrivilege_list(ctx *Privilege_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPrivilege(ctx *PrivilegeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPrivilege_target(ctx *Privilege_targetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGrantee_list(ctx *Grantee_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGrantee(ctx *GranteeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGrant_grant_option_(ctx *Grant_grant_option_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGrantrolestmt(ctx *GrantrolestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRevokerolestmt(ctx *RevokerolestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGrant_admin_option_(ctx *Grant_admin_option_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGranted_by_(ctx *Granted_by_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterdefaultprivilegesstmt(ctx *AlterdefaultprivilegesstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDefacloptionlist(ctx *DefacloptionlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDefacloption(ctx *DefacloptionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDefaclaction(ctx *DefaclactionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDefacl_privilege_target(ctx *Defacl_privilege_targetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndexstmt(ctx *IndexstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUnique_(ctx *Unique_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSingle_name_(ctx *Single_name_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConcurrently_(ctx *Concurrently_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndex_name_(ctx *Index_name_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAccess_method_clause(ctx *Access_method_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndex_params(ctx *Index_paramsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndex_elem_options(ctx *Index_elem_optionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndex_elem(ctx *Index_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInclude_(ctx *Include_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndex_including_params(ctx *Index_including_paramsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCollate_(ctx *Collate_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitClass_(ctx *Class_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAsc_desc_(ctx *Asc_desc_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNulls_order_(ctx *Nulls_order_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatefunctionstmt(ctx *CreatefunctionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOr_replace_(ctx *Or_replace_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_args(ctx *Func_argsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_args_list(ctx *Func_args_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunction_with_argtypes_list(ctx *Function_with_argtypes_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunction_with_argtypes(ctx *Function_with_argtypesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_args_with_defaults(ctx *Func_args_with_defaultsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_args_with_defaults_list(ctx *Func_args_with_defaults_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_arg(ctx *Func_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitArg_class(ctx *Arg_classContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitParam_name(ctx *Param_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_return(ctx *Func_returnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_type(ctx *Func_typeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_arg_with_default(ctx *Func_arg_with_defaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAggr_arg(ctx *Aggr_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAggr_args(ctx *Aggr_argsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAggr_args_list(ctx *Aggr_args_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAggregate_with_argtypes(ctx *Aggregate_with_argtypesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAggregate_with_argtypes_list(ctx *Aggregate_with_argtypes_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatefunc_opt_list(ctx *Createfunc_opt_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCommon_func_opt_item(ctx *Common_func_opt_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatefunc_opt_item(ctx *Createfunc_opt_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_as(ctx *Func_asContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransform_type_list(ctx *Transform_type_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDefinition_(ctx *Definition_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTable_func_column(ctx *Table_func_columnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTable_func_column_list(ctx *Table_func_column_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterfunctionstmt(ctx *AlterfunctionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterfunc_opt_list(ctx *Alterfunc_opt_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRestrict_(ctx *Restrict_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRemovefuncstmt(ctx *RemovefuncstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRemoveaggrstmt(ctx *RemoveaggrstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRemoveoperstmt(ctx *RemoveoperstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOper_argtypes(ctx *Oper_argtypesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAny_operator(ctx *Any_operatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOperator_with_argtypes_list(ctx *Operator_with_argtypes_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOperator_with_argtypes(ctx *Operator_with_argtypesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDostmt(ctx *DostmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDostmt_opt_list(ctx *Dostmt_opt_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDostmt_opt_item(ctx *Dostmt_opt_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatecaststmt(ctx *CreatecaststmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCast_context(ctx *Cast_contextContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDropcaststmt(ctx *DropcaststmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIf_exists_(ctx *If_exists_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatetransformstmt(ctx *CreatetransformstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransform_element_list(ctx *Transform_element_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDroptransformstmt(ctx *DroptransformstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReindexstmt(ctx *ReindexstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReindex_target_relation(ctx *Reindex_target_relationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReindex_target_all(ctx *Reindex_target_allContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReindex_option_list(ctx *Reindex_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltertblspcstmt(ctx *AltertblspcstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRenamestmt(ctx *RenamestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColumn_(ctx *Column_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSet_data_(ctx *Set_data_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterobjectdependsstmt(ctx *AlterobjectdependsstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNo_(ctx *No_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterobjectschemastmt(ctx *AlterobjectschemastmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlteroperatorstmt(ctx *AlteroperatorstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOperator_def_list(ctx *Operator_def_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOperator_def_elem(ctx *Operator_def_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOperator_def_arg(ctx *Operator_def_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltertypestmt(ctx *AltertypestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterownerstmt(ctx *AlterownerstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatepublicationstmt(ctx *CreatepublicationstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPublication_for_tables_(ctx *Publication_for_tables_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPublication_for_tables(ctx *Publication_for_tablesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterpublicationstmt(ctx *AlterpublicationstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatesubscriptionstmt(ctx *CreatesubscriptionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPublication_name_list(ctx *Publication_name_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPublication_name_item(ctx *Publication_name_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltersubscriptionstmt(ctx *AltersubscriptionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDropsubscriptionstmt(ctx *DropsubscriptionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRulestmt(ctx *RulestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRuleactionlist(ctx *RuleactionlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRuleactionmulti(ctx *RuleactionmultiContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRuleactionstmt(ctx *RuleactionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRuleactionstmtOrEmpty(ctx *RuleactionstmtOrEmptyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEvent(ctx *EventContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInstead_(ctx *Instead_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNotifystmt(ctx *NotifystmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNotify_payload(ctx *Notify_payloadContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitListenstmt(ctx *ListenstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUnlistenstmt(ctx *UnlistenstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransactionstmt(ctx *TransactionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransaction_(ctx *Transaction_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransaction_mode_item(ctx *Transaction_mode_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransaction_mode_list(ctx *Transaction_mode_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransaction_mode_list_or_empty(ctx *Transaction_mode_list_or_emptyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTransaction_chain_(ctx *Transaction_chain_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitViewstmt(ctx *ViewstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCheck_option_(ctx *Check_option_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitLoadstmt(ctx *LoadstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatedbstmt(ctx *CreatedbstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatedb_opt_list(ctx *Createdb_opt_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatedb_opt_items(ctx *Createdb_opt_itemsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatedb_opt_item(ctx *Createdb_opt_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatedb_opt_name(ctx *Createdb_opt_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEqual_(ctx *Equal_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterdatabasestmt(ctx *AlterdatabasestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterdatabasesetstmt(ctx *AlterdatabasesetstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDropdbstmt(ctx *DropdbstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDrop_option_list(ctx *Drop_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDrop_option(ctx *Drop_optionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltercollationstmt(ctx *AltercollationstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltersystemstmt(ctx *AltersystemstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreatedomainstmt(ctx *CreatedomainstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlterdomainstmt(ctx *AlterdomainstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAs_(ctx *As_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltertsdictionarystmt(ctx *AltertsdictionarystmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAltertsconfigurationstmt(ctx *AltertsconfigurationstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAny_with(ctx *Any_withContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCreateconversionstmt(ctx *CreateconversionstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitClusterstmt(ctx *ClusterstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCluster_index_specification(ctx *Cluster_index_specificationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVacuumstmt(ctx *VacuumstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAnalyzestmt(ctx *AnalyzestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUtility_option_list(ctx *Utility_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVac_analyze_option_list(ctx *Vac_analyze_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAnalyze_keyword(ctx *Analyze_keywordContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUtility_option_elem(ctx *Utility_option_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUtility_option_name(ctx *Utility_option_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUtility_option_arg(ctx *Utility_option_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVac_analyze_option_elem(ctx *Vac_analyze_option_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVac_analyze_option_name(ctx *Vac_analyze_option_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVac_analyze_option_arg(ctx *Vac_analyze_option_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAnalyze_(ctx *Analyze_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVerbose_(ctx *Verbose_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFull_(ctx *Full_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFreeze_(ctx *Freeze_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitName_list_(ctx *Name_list_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVacuum_relation(ctx *Vacuum_relationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVacuum_relation_list(ctx *Vacuum_relation_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVacuum_relation_list_(ctx *Vacuum_relation_list_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExplainstmt(ctx *ExplainstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExplainablestmt(ctx *ExplainablestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExplain_option_list(ctx *Explain_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExplain_option_elem(ctx *Explain_option_elemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExplain_option_name(ctx *Explain_option_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExplain_option_arg(ctx *Explain_option_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPreparestmt(ctx *PreparestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPrep_type_clause(ctx *Prep_type_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPreparablestmt(ctx *PreparablestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExecutestmt(ctx *ExecutestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExecute_param_clause(ctx *Execute_param_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDeallocatestmt(ctx *DeallocatestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInsertstmt(ctx *InsertstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInsert_target(ctx *Insert_targetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInsert_rest(ctx *Insert_restContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOverride_kind(ctx *Override_kindContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInsert_column_list(ctx *Insert_column_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInsert_column_item(ctx *Insert_column_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOn_conflict_(ctx *On_conflict_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConf_expr_(ctx *Conf_expr_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReturning_clause(ctx *Returning_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitMergestmt(ctx *MergestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitMerge_insert_clause(ctx *Merge_insert_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitMerge_update_clause(ctx *Merge_update_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitMerge_delete_clause(ctx *Merge_delete_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDeletestmt(ctx *DeletestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUsing_clause(ctx *Using_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitLockstmt(ctx *LockstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitLock_(ctx *Lock_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitLock_type(ctx *Lock_typeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNowait_(ctx *Nowait_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNowait_or_skip_(ctx *Nowait_or_skip_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUpdatestmt(ctx *UpdatestmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSet_clause_list(ctx *Set_clause_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSet_clause(ctx *Set_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSet_target(ctx *Set_targetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSet_target_list(ctx *Set_target_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDeclarecursorstmt(ctx *DeclarecursorstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCursor_name(ctx *Cursor_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCursor_options(ctx *Cursor_optionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitHold_(ctx *Hold_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelectstmt(ctx *SelectstmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelect_with_parens(ctx *Select_with_parensContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelect_no_parens(ctx *Select_no_parensContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelect_clause(ctx *Select_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSimple_select_intersect(ctx *Simple_select_intersectContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSimple_select_pramary(ctx *Simple_select_pramaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWith_clause(ctx *With_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCte_list(ctx *Cte_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCommon_table_expr(ctx *Common_table_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitMaterialized_(ctx *Materialized_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWith_clause_(ctx *With_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInto_clause(ctx *Into_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitStrict_(ctx *Strict_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpttempTableName(ctx *OpttempTableNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTable_(ctx *Table_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAll_or_distinct(ctx *All_or_distinctContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDistinct_clause(ctx *Distinct_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAll_clause_(ctx *All_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSort_clause_(ctx *Sort_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSort_clause(ctx *Sort_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSortby_list(ctx *Sortby_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSortby(ctx *SortbyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelect_limit(ctx *Select_limitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelect_limit_(ctx *Select_limit_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitLimit_clause(ctx *Limit_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOffset_clause(ctx *Offset_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelect_limit_value(ctx *Select_limit_valueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelect_offset_value(ctx *Select_offset_valueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSelect_fetch_first_value(ctx *Select_fetch_first_valueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitI_or_f_const(ctx *I_or_f_constContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRow_or_rows(ctx *Row_or_rowsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFirst_or_next(ctx *First_or_nextContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGroup_clause(ctx *Group_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGroup_by_list(ctx *Group_by_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGroup_by_item(ctx *Group_by_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEmpty_grouping_set(ctx *Empty_grouping_setContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRollup_clause(ctx *Rollup_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCube_clause(ctx *Cube_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGrouping_sets_clause(ctx *Grouping_sets_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitHaving_clause(ctx *Having_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFor_locking_clause(ctx *For_locking_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFor_locking_clause_(ctx *For_locking_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFor_locking_items(ctx *For_locking_itemsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFor_locking_item(ctx *For_locking_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFor_locking_strength(ctx *For_locking_strengthContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitLocked_rels_list(ctx *Locked_rels_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitValues_clause(ctx *Values_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFrom_clause(ctx *From_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFrom_list(ctx *From_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTable_ref(ctx *Table_refContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAlias_clause(ctx *Alias_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_alias_clause(ctx *Func_alias_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJoin_type(ctx *Join_typeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJoin_qual(ctx *Join_qualContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRelation_expr(ctx *Relation_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRelation_expr_list(ctx *Relation_expr_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRelation_expr_opt_alias(ctx *Relation_expr_opt_aliasContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTablesample_clause(ctx *Tablesample_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRepeatable_clause_(ctx *Repeatable_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_table(ctx *Func_tableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRowsfrom_item(ctx *Rowsfrom_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRowsfrom_list(ctx *Rowsfrom_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCol_def_list_(ctx *Col_def_list_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOrdinality_(ctx *Ordinality_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWhere_clause(ctx *Where_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWhere_or_current_clause(ctx *Where_or_current_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpttablefuncelementlist(ctx *OpttablefuncelementlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTablefuncelementlist(ctx *TablefuncelementlistContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTablefuncelement(ctx *TablefuncelementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXmltable(ctx *XmltableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXmltable_column_list(ctx *Xmltable_column_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXmltable_column_el(ctx *Xmltable_column_elContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXmltable_column_option_list(ctx *Xmltable_column_option_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXmltable_column_option_el(ctx *Xmltable_column_option_elContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_namespace_list(ctx *Xml_namespace_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_namespace_el(ctx *Xml_namespace_elContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTypename(ctx *TypenameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpt_array_bounds(ctx *Opt_array_boundsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSimpletypename(ctx *SimpletypenameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConsttypename(ctx *ConsttypenameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitGenerictype(ctx *GenerictypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitType_modifiers_(ctx *Type_modifiers_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNumeric(ctx *NumericContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFloat_(ctx *Float_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBit(ctx *BitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstbit(ctx *ConstbitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBitwithlength(ctx *BitwithlengthContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBitwithoutlength(ctx *BitwithoutlengthContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCharacter(ctx *CharacterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstcharacter(ctx *ConstcharacterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCharacter_c(ctx *Character_cContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitVarying_(ctx *Varying_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstdatetime(ctx *ConstdatetimeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitConstinterval(ctx *ConstintervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTimezone_(ctx *Timezone_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInterval_(ctx *Interval_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitInterval_second(ctx *Interval_secondContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJsonType(ctx *JsonTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitEscape_(ctx *Escape_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr(ctx *A_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_qual(ctx *A_expr_qualContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_lessless(ctx *A_expr_lesslessContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_or(ctx *A_expr_orContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_and(ctx *A_expr_andContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_between(ctx *A_expr_betweenContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_in(ctx *A_expr_inContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_unary_not(ctx *A_expr_unary_notContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_isnull(ctx *A_expr_isnullContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_is_not(ctx *A_expr_is_notContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_compare(ctx *A_expr_compareContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_like(ctx *A_expr_likeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_qual_op(ctx *A_expr_qual_opContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_unary_qualop(ctx *A_expr_unary_qualopContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_add(ctx *A_expr_addContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_mul(ctx *A_expr_mulContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_caret(ctx *A_expr_caretContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_unary_sign(ctx *A_expr_unary_signContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_at_time_zone(ctx *A_expr_at_time_zoneContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_collate(ctx *A_expr_collateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitA_expr_typecast(ctx *A_expr_typecastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitB_expr(ctx *B_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitC_expr_exists(ctx *C_expr_existsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitC_expr_expr(ctx *C_expr_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitC_expr_case(ctx *C_expr_caseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPlsqlvariablename(ctx *PlsqlvariablenameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_application(ctx *Func_applicationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_expr(ctx *Func_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_expr_windowless(ctx *Func_expr_windowlessContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_expr_common_subexpr(ctx *Func_expr_common_subexprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_root_version(ctx *Xml_root_versionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_root_standalone_(ctx *Xml_root_standalone_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_attributes(ctx *Xml_attributesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_attribute_list(ctx *Xml_attribute_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_attribute_el(ctx *Xml_attribute_elContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitDocument_or_content(ctx *Document_or_contentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_whitespace_option(ctx *Xml_whitespace_optionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXmlexists_argument(ctx *Xmlexists_argumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXml_passing_mech(ctx *Xml_passing_mechContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWithin_group_clause(ctx *Within_group_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFilter_clause(ctx *Filter_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWindow_clause(ctx *Window_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWindow_definition_list(ctx *Window_definition_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWindow_definition(ctx *Window_definitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOver_clause(ctx *Over_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWindow_specification(ctx *Window_specificationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExisting_window_name_(ctx *Existing_window_name_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPartition_clause_(ctx *Partition_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFrame_clause_(ctx *Frame_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFrame_extent(ctx *Frame_extentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFrame_bound(ctx *Frame_boundContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWindow_exclusion_clause_(ctx *Window_exclusion_clause_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRow(ctx *RowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExplicit_row(ctx *Explicit_rowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitImplicit_row(ctx *Implicit_rowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSub_type(ctx *Sub_typeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAll_op(ctx *All_opContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitMathop(ctx *MathopContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitQual_op(ctx *Qual_opContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitQual_all_op(ctx *Qual_all_opContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSubquery_Op(ctx *Subquery_OpContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExpr_list(ctx *Expr_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_arg_list(ctx *Func_arg_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_arg_expr(ctx *Func_arg_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitType_list(ctx *Type_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitArray_expr(ctx *Array_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitArray_expr_list(ctx *Array_expr_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExtract_list(ctx *Extract_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitExtract_arg(ctx *Extract_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUnicode_normal_form(ctx *Unicode_normal_formContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOverlay_list(ctx *Overlay_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitPosition_list(ctx *Position_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSubstr_list(ctx *Substr_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTrim_list(ctx *Trim_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIn_expr_select(ctx *In_expr_selectContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIn_expr_list(ctx *In_expr_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCase_expr(ctx *Case_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWhen_clause_list(ctx *When_clause_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitWhen_clause(ctx *When_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCase_default(ctx *Case_defaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCase_arg(ctx *Case_argContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColumnref(ctx *ColumnrefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndirection_el(ctx *Indirection_elContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSlice_bound_(ctx *Slice_bound_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIndirection(ctx *IndirectionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitOpt_indirection(ctx *Opt_indirectionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_passing_clause(ctx *Json_passing_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_arguments(ctx *Json_argumentsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_argument(ctx *Json_argumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_wrapper_behavior(ctx *Json_wrapper_behaviorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_behavior(ctx *Json_behaviorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_behavior_type(ctx *Json_behavior_typeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_behavior_clause(ctx *Json_behavior_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_on_error_clause(ctx *Json_on_error_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_value_expr(ctx *Json_value_exprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_format_clause(ctx *Json_format_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_quotes_clause(ctx *Json_quotes_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_returning_clause(ctx *Json_returning_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_predicate_type_constraint(ctx *Json_predicate_type_constraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_key_uniqueness_constraint(ctx *Json_key_uniqueness_constraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_name_and_value_list(ctx *Json_name_and_value_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_name_and_value(ctx *Json_name_and_valueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_object_constructor_null_clause(ctx *Json_object_constructor_null_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_array_constructor_null_clause(ctx *Json_array_constructor_null_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_value_expr_list(ctx *Json_value_expr_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_aggregate_func(ctx *Json_aggregate_funcContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitJson_array_aggregate_order_by_clause(ctx *Json_array_aggregate_order_by_clauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTarget_list_(ctx *Target_list_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTarget_list(ctx *Target_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTarget_label(ctx *Target_labelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitTarget_star(ctx *Target_starContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitQualified_name_list(ctx *Qualified_name_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitQualified_name(ctx *Qualified_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitName_list(ctx *Name_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitName(ctx *NameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAttr_name(ctx *Attr_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFile_name(ctx *File_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFunc_name(ctx *Func_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAexprconst(ctx *AexprconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitXconst(ctx *XconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBconst(ctx *BconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitFconst(ctx *FconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIconst(ctx *IconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSconst(ctx *SconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAnysconst(ctx *AnysconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUescape_(ctx *Uescape_Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitSignediconst(ctx *SignediconstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRoleid(ctx *RoleidContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRolespec(ctx *RolespecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitRole_list(ctx *Role_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColid(ctx *ColidContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitType_function_name(ctx *Type_function_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitNonreservedword(ctx *NonreservedwordContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitColLabel(ctx *ColLabelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBareColLabel(ctx *BareColLabelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitUnreserved_keyword(ctx *Unreserved_keywordContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitCol_name_keyword(ctx *Col_name_keywordContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitType_func_name_keyword(ctx *Type_func_name_keywordContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitReserved_keyword(ctx *Reserved_keywordContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitBare_label_keyword(ctx *Bare_label_keywordContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitAny_identifier(ctx *Any_identifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePostgreSQLParserVisitor) VisitIdentifier(ctx *IdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}
