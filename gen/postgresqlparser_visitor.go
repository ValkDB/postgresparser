// Code generated from PostgreSQLParser.g4 by ANTLR 4.13.1. DO NOT EDIT.

package gen // PostgreSQLParser
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by PostgreSQLParser.
type PostgreSQLParserVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by PostgreSQLParser#root.
	VisitRoot(ctx *RootContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#stmtblock.
	VisitStmtblock(ctx *StmtblockContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#stmtmulti.
	VisitStmtmulti(ctx *StmtmultiContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#stmt.
	VisitStmt(ctx *StmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#callstmt.
	VisitCallstmt(ctx *CallstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createrolestmt.
	VisitCreaterolestmt(ctx *CreaterolestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#with_.
	VisitWith_(ctx *With_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optrolelist.
	VisitOptrolelist(ctx *OptrolelistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alteroptrolelist.
	VisitAlteroptrolelist(ctx *AlteroptrolelistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alteroptroleelem.
	VisitAlteroptroleelem(ctx *AlteroptroleelemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createoptroleelem.
	VisitCreateoptroleelem(ctx *CreateoptroleelemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createuserstmt.
	VisitCreateuserstmt(ctx *CreateuserstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterrolestmt.
	VisitAlterrolestmt(ctx *AlterrolestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#in_database_.
	VisitIn_database_(ctx *In_database_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterrolesetstmt.
	VisitAlterrolesetstmt(ctx *AlterrolesetstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#droprolestmt.
	VisitDroprolestmt(ctx *DroprolestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#creategroupstmt.
	VisitCreategroupstmt(ctx *CreategroupstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altergroupstmt.
	VisitAltergroupstmt(ctx *AltergroupstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#add_drop.
	VisitAdd_drop(ctx *Add_dropContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createschemastmt.
	VisitCreateschemastmt(ctx *CreateschemastmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optschemaname.
	VisitOptschemaname(ctx *OptschemanameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optschemaeltlist.
	VisitOptschemaeltlist(ctx *OptschemaeltlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#schema_stmt.
	VisitSchema_stmt(ctx *Schema_stmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#variablesetstmt.
	VisitVariablesetstmt(ctx *VariablesetstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#set_rest.
	VisitSet_rest(ctx *Set_restContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#generic_set.
	VisitGeneric_set(ctx *Generic_setContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#set_rest_more.
	VisitSet_rest_more(ctx *Set_rest_moreContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#var_name.
	VisitVar_name(ctx *Var_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#var_list.
	VisitVar_list(ctx *Var_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#var_value.
	VisitVar_value(ctx *Var_valueContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#iso_level.
	VisitIso_level(ctx *Iso_levelContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#boolean_or_string_.
	VisitBoolean_or_string_(ctx *Boolean_or_string_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#zone_value.
	VisitZone_value(ctx *Zone_valueContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#encoding_.
	VisitEncoding_(ctx *Encoding_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#nonreservedword_or_sconst.
	VisitNonreservedword_or_sconst(ctx *Nonreservedword_or_sconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#variableresetstmt.
	VisitVariableresetstmt(ctx *VariableresetstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reset_rest.
	VisitReset_rest(ctx *Reset_restContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#generic_reset.
	VisitGeneric_reset(ctx *Generic_resetContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#setresetclause.
	VisitSetresetclause(ctx *SetresetclauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#functionsetresetclause.
	VisitFunctionsetresetclause(ctx *FunctionsetresetclauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#variableshowstmt.
	VisitVariableshowstmt(ctx *VariableshowstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constraintssetstmt.
	VisitConstraintssetstmt(ctx *ConstraintssetstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constraints_set_list.
	VisitConstraints_set_list(ctx *Constraints_set_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constraints_set_mode.
	VisitConstraints_set_mode(ctx *Constraints_set_modeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#checkpointstmt.
	VisitCheckpointstmt(ctx *CheckpointstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#discardstmt.
	VisitDiscardstmt(ctx *DiscardstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altertablestmt.
	VisitAltertablestmt(ctx *AltertablestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_table_cmds.
	VisitAlter_table_cmds(ctx *Alter_table_cmdsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#partition_cmd.
	VisitPartition_cmd(ctx *Partition_cmdContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#index_partition_cmd.
	VisitIndex_partition_cmd(ctx *Index_partition_cmdContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_table_cmd.
	VisitAlter_table_cmd(ctx *Alter_table_cmdContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_column_default.
	VisitAlter_column_default(ctx *Alter_column_defaultContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#drop_behavior_.
	VisitDrop_behavior_(ctx *Drop_behavior_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#collate_clause_.
	VisitCollate_clause_(ctx *Collate_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_using.
	VisitAlter_using(ctx *Alter_usingContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#replica_identity.
	VisitReplica_identity(ctx *Replica_identityContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reloptions.
	VisitReloptions(ctx *ReloptionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reloptions_.
	VisitReloptions_(ctx *Reloptions_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reloption_list.
	VisitReloption_list(ctx *Reloption_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reloption_elem.
	VisitReloption_elem(ctx *Reloption_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_identity_column_option_list.
	VisitAlter_identity_column_option_list(ctx *Alter_identity_column_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_identity_column_option.
	VisitAlter_identity_column_option(ctx *Alter_identity_column_optionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#partitionboundspec.
	VisitPartitionboundspec(ctx *PartitionboundspecContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#hash_partbound_elem.
	VisitHash_partbound_elem(ctx *Hash_partbound_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#hash_partbound.
	VisitHash_partbound(ctx *Hash_partboundContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altercompositetypestmt.
	VisitAltercompositetypestmt(ctx *AltercompositetypestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_type_cmds.
	VisitAlter_type_cmds(ctx *Alter_type_cmdsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_type_cmd.
	VisitAlter_type_cmd(ctx *Alter_type_cmdContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#closeportalstmt.
	VisitCloseportalstmt(ctx *CloseportalstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copystmt.
	VisitCopystmt(ctx *CopystmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_from.
	VisitCopy_from(ctx *Copy_fromContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#program_.
	VisitProgram_(ctx *Program_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_file_name.
	VisitCopy_file_name(ctx *Copy_file_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_options.
	VisitCopy_options(ctx *Copy_optionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_opt_list.
	VisitCopy_opt_list(ctx *Copy_opt_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_opt_item.
	VisitCopy_opt_item(ctx *Copy_opt_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#binary_.
	VisitBinary_(ctx *Binary_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_delimiter.
	VisitCopy_delimiter(ctx *Copy_delimiterContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#using_.
	VisitUsing_(ctx *Using_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_generic_opt_list.
	VisitCopy_generic_opt_list(ctx *Copy_generic_opt_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_generic_opt_elem.
	VisitCopy_generic_opt_elem(ctx *Copy_generic_opt_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_generic_opt_arg.
	VisitCopy_generic_opt_arg(ctx *Copy_generic_opt_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_generic_opt_arg_list.
	VisitCopy_generic_opt_arg_list(ctx *Copy_generic_opt_arg_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#copy_generic_opt_arg_list_item.
	VisitCopy_generic_opt_arg_list_item(ctx *Copy_generic_opt_arg_list_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createstmt.
	VisitCreatestmt(ctx *CreatestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opttemp.
	VisitOpttemp(ctx *OpttempContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opttableelementlist.
	VisitOpttableelementlist(ctx *OpttableelementlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opttypedtableelementlist.
	VisitOpttypedtableelementlist(ctx *OpttypedtableelementlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tableelementlist.
	VisitTableelementlist(ctx *TableelementlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#typedtableelementlist.
	VisitTypedtableelementlist(ctx *TypedtableelementlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tableelement.
	VisitTableelement(ctx *TableelementContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#typedtableelement.
	VisitTypedtableelement(ctx *TypedtableelementContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#columnDef.
	VisitColumnDef(ctx *ColumnDefContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#columnOptions.
	VisitColumnOptions(ctx *ColumnOptionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#colquallist.
	VisitColquallist(ctx *ColquallistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#colconstraint.
	VisitColconstraint(ctx *ColconstraintContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#colconstraintelem.
	VisitColconstraintelem(ctx *ColconstraintelemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#generated_when.
	VisitGenerated_when(ctx *Generated_whenContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constraintattr.
	VisitConstraintattr(ctx *ConstraintattrContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tablelikeclause.
	VisitTablelikeclause(ctx *TablelikeclauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tablelikeoptionlist.
	VisitTablelikeoptionlist(ctx *TablelikeoptionlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tablelikeoption.
	VisitTablelikeoption(ctx *TablelikeoptionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tableconstraint.
	VisitTableconstraint(ctx *TableconstraintContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constraintelem.
	VisitConstraintelem(ctx *ConstraintelemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#no_inherit_.
	VisitNo_inherit_(ctx *No_inherit_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#column_list_.
	VisitColumn_list_(ctx *Column_list_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#columnlist.
	VisitColumnlist(ctx *ColumnlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#columnElem.
	VisitColumnElem(ctx *ColumnElemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#c_include_.
	VisitC_include_(ctx *C_include_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#key_match.
	VisitKey_match(ctx *Key_matchContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#exclusionconstraintlist.
	VisitExclusionconstraintlist(ctx *ExclusionconstraintlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#exclusionconstraintelem.
	VisitExclusionconstraintelem(ctx *ExclusionconstraintelemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#exclusionwhereclause.
	VisitExclusionwhereclause(ctx *ExclusionwhereclauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#key_actions.
	VisitKey_actions(ctx *Key_actionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#key_update.
	VisitKey_update(ctx *Key_updateContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#key_delete.
	VisitKey_delete(ctx *Key_deleteContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#key_action.
	VisitKey_action(ctx *Key_actionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optinherit.
	VisitOptinherit(ctx *OptinheritContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optpartitionspec.
	VisitOptpartitionspec(ctx *OptpartitionspecContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#partitionspec.
	VisitPartitionspec(ctx *PartitionspecContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#part_params.
	VisitPart_params(ctx *Part_paramsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#part_elem.
	VisitPart_elem(ctx *Part_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#table_access_method_clause.
	VisitTable_access_method_clause(ctx *Table_access_method_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optwith.
	VisitOptwith(ctx *OptwithContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#oncommitoption.
	VisitOncommitoption(ctx *OncommitoptionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opttablespace.
	VisitOpttablespace(ctx *OpttablespaceContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optconstablespace.
	VisitOptconstablespace(ctx *OptconstablespaceContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#existingindex.
	VisitExistingindex(ctx *ExistingindexContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createstatsstmt.
	VisitCreatestatsstmt(ctx *CreatestatsstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterstatsstmt.
	VisitAlterstatsstmt(ctx *AlterstatsstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createasstmt.
	VisitCreateasstmt(ctx *CreateasstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#create_as_target.
	VisitCreate_as_target(ctx *Create_as_targetContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#with_data_.
	VisitWith_data_(ctx *With_data_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#creatematviewstmt.
	VisitCreatematviewstmt(ctx *CreatematviewstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#create_mv_target.
	VisitCreate_mv_target(ctx *Create_mv_targetContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optnolog.
	VisitOptnolog(ctx *OptnologContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#refreshmatviewstmt.
	VisitRefreshmatviewstmt(ctx *RefreshmatviewstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createseqstmt.
	VisitCreateseqstmt(ctx *CreateseqstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterseqstmt.
	VisitAlterseqstmt(ctx *AlterseqstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optseqoptlist.
	VisitOptseqoptlist(ctx *OptseqoptlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optparenthesizedseqoptlist.
	VisitOptparenthesizedseqoptlist(ctx *OptparenthesizedseqoptlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#seqoptlist.
	VisitSeqoptlist(ctx *SeqoptlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#seqoptelem.
	VisitSeqoptelem(ctx *SeqoptelemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#by_.
	VisitBy_(ctx *By_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#numericonly.
	VisitNumericonly(ctx *NumericonlyContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#numericonly_list.
	VisitNumericonly_list(ctx *Numericonly_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createplangstmt.
	VisitCreateplangstmt(ctx *CreateplangstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#trusted_.
	VisitTrusted_(ctx *Trusted_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#handler_name.
	VisitHandler_name(ctx *Handler_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#inline_handler_.
	VisitInline_handler_(ctx *Inline_handler_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#validator_clause.
	VisitValidator_clause(ctx *Validator_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#validator_.
	VisitValidator_(ctx *Validator_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#procedural_.
	VisitProcedural_(ctx *Procedural_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createtablespacestmt.
	VisitCreatetablespacestmt(ctx *CreatetablespacestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opttablespaceowner.
	VisitOpttablespaceowner(ctx *OpttablespaceownerContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#droptablespacestmt.
	VisitDroptablespacestmt(ctx *DroptablespacestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createextensionstmt.
	VisitCreateextensionstmt(ctx *CreateextensionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#create_extension_opt_list.
	VisitCreate_extension_opt_list(ctx *Create_extension_opt_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#create_extension_opt_item.
	VisitCreate_extension_opt_item(ctx *Create_extension_opt_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterextensionstmt.
	VisitAlterextensionstmt(ctx *AlterextensionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_extension_opt_list.
	VisitAlter_extension_opt_list(ctx *Alter_extension_opt_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_extension_opt_item.
	VisitAlter_extension_opt_item(ctx *Alter_extension_opt_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterextensioncontentsstmt.
	VisitAlterextensioncontentsstmt(ctx *AlterextensioncontentsstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createfdwstmt.
	VisitCreatefdwstmt(ctx *CreatefdwstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#fdw_option.
	VisitFdw_option(ctx *Fdw_optionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#fdw_options.
	VisitFdw_options(ctx *Fdw_optionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#fdw_options_.
	VisitFdw_options_(ctx *Fdw_options_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterfdwstmt.
	VisitAlterfdwstmt(ctx *AlterfdwstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#create_generic_options.
	VisitCreate_generic_options(ctx *Create_generic_optionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#generic_option_list.
	VisitGeneric_option_list(ctx *Generic_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_generic_options.
	VisitAlter_generic_options(ctx *Alter_generic_optionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_generic_option_list.
	VisitAlter_generic_option_list(ctx *Alter_generic_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alter_generic_option_elem.
	VisitAlter_generic_option_elem(ctx *Alter_generic_option_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#generic_option_elem.
	VisitGeneric_option_elem(ctx *Generic_option_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#generic_option_name.
	VisitGeneric_option_name(ctx *Generic_option_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#generic_option_arg.
	VisitGeneric_option_arg(ctx *Generic_option_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createforeignserverstmt.
	VisitCreateforeignserverstmt(ctx *CreateforeignserverstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#type_.
	VisitType_(ctx *Type_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#foreign_server_version.
	VisitForeign_server_version(ctx *Foreign_server_versionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#foreign_server_version_.
	VisitForeign_server_version_(ctx *Foreign_server_version_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterforeignserverstmt.
	VisitAlterforeignserverstmt(ctx *AlterforeignserverstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createforeigntablestmt.
	VisitCreateforeigntablestmt(ctx *CreateforeigntablestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#importforeignschemastmt.
	VisitImportforeignschemastmt(ctx *ImportforeignschemastmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#import_qualification_type.
	VisitImport_qualification_type(ctx *Import_qualification_typeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#import_qualification.
	VisitImport_qualification(ctx *Import_qualificationContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createusermappingstmt.
	VisitCreateusermappingstmt(ctx *CreateusermappingstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#auth_ident.
	VisitAuth_ident(ctx *Auth_identContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dropusermappingstmt.
	VisitDropusermappingstmt(ctx *DropusermappingstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterusermappingstmt.
	VisitAlterusermappingstmt(ctx *AlterusermappingstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createpolicystmt.
	VisitCreatepolicystmt(ctx *CreatepolicystmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterpolicystmt.
	VisitAlterpolicystmt(ctx *AlterpolicystmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rowsecurityoptionalexpr.
	VisitRowsecurityoptionalexpr(ctx *RowsecurityoptionalexprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rowsecurityoptionalwithcheck.
	VisitRowsecurityoptionalwithcheck(ctx *RowsecurityoptionalwithcheckContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rowsecuritydefaulttorole.
	VisitRowsecuritydefaulttorole(ctx *RowsecuritydefaulttoroleContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rowsecurityoptionaltorole.
	VisitRowsecurityoptionaltorole(ctx *RowsecurityoptionaltoroleContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rowsecuritydefaultpermissive.
	VisitRowsecuritydefaultpermissive(ctx *RowsecuritydefaultpermissiveContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rowsecuritydefaultforcmd.
	VisitRowsecuritydefaultforcmd(ctx *RowsecuritydefaultforcmdContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#row_security_cmd.
	VisitRow_security_cmd(ctx *Row_security_cmdContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createamstmt.
	VisitCreateamstmt(ctx *CreateamstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#am_type.
	VisitAm_type(ctx *Am_typeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createtrigstmt.
	VisitCreatetrigstmt(ctx *CreatetrigstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggeractiontime.
	VisitTriggeractiontime(ctx *TriggeractiontimeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggerevents.
	VisitTriggerevents(ctx *TriggereventsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggeroneevent.
	VisitTriggeroneevent(ctx *TriggeroneeventContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggerreferencing.
	VisitTriggerreferencing(ctx *TriggerreferencingContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggertransitions.
	VisitTriggertransitions(ctx *TriggertransitionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggertransition.
	VisitTriggertransition(ctx *TriggertransitionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transitionoldornew.
	VisitTransitionoldornew(ctx *TransitionoldornewContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transitionrowortable.
	VisitTransitionrowortable(ctx *TransitionrowortableContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transitionrelname.
	VisitTransitionrelname(ctx *TransitionrelnameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggerforspec.
	VisitTriggerforspec(ctx *TriggerforspecContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggerforopteach.
	VisitTriggerforopteach(ctx *TriggerforopteachContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggerfortype.
	VisitTriggerfortype(ctx *TriggerfortypeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggerwhen.
	VisitTriggerwhen(ctx *TriggerwhenContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#function_or_procedure.
	VisitFunction_or_procedure(ctx *Function_or_procedureContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggerfuncargs.
	VisitTriggerfuncargs(ctx *TriggerfuncargsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#triggerfuncarg.
	VisitTriggerfuncarg(ctx *TriggerfuncargContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#optconstrfromtable.
	VisitOptconstrfromtable(ctx *OptconstrfromtableContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constraintattributespec.
	VisitConstraintattributespec(ctx *ConstraintattributespecContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constraintattributeElem.
	VisitConstraintattributeElem(ctx *ConstraintattributeElemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createeventtrigstmt.
	VisitCreateeventtrigstmt(ctx *CreateeventtrigstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#event_trigger_when_list.
	VisitEvent_trigger_when_list(ctx *Event_trigger_when_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#event_trigger_when_item.
	VisitEvent_trigger_when_item(ctx *Event_trigger_when_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#event_trigger_value_list.
	VisitEvent_trigger_value_list(ctx *Event_trigger_value_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altereventtrigstmt.
	VisitAltereventtrigstmt(ctx *AltereventtrigstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#enable_trigger.
	VisitEnable_trigger(ctx *Enable_triggerContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createassertionstmt.
	VisitCreateassertionstmt(ctx *CreateassertionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#definestmt.
	VisitDefinestmt(ctx *DefinestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#definition.
	VisitDefinition(ctx *DefinitionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#def_list.
	VisitDef_list(ctx *Def_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#def_elem.
	VisitDef_elem(ctx *Def_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#def_arg.
	VisitDef_arg(ctx *Def_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#old_aggr_definition.
	VisitOld_aggr_definition(ctx *Old_aggr_definitionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#old_aggr_list.
	VisitOld_aggr_list(ctx *Old_aggr_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#old_aggr_elem.
	VisitOld_aggr_elem(ctx *Old_aggr_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#enum_val_list_.
	VisitEnum_val_list_(ctx *Enum_val_list_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#enum_val_list.
	VisitEnum_val_list(ctx *Enum_val_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterenumstmt.
	VisitAlterenumstmt(ctx *AlterenumstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#if_not_exists_.
	VisitIf_not_exists_(ctx *If_not_exists_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createopclassstmt.
	VisitCreateopclassstmt(ctx *CreateopclassstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opclass_item_list.
	VisitOpclass_item_list(ctx *Opclass_item_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opclass_item.
	VisitOpclass_item(ctx *Opclass_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#default_.
	VisitDefault_(ctx *Default_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opfamily_.
	VisitOpfamily_(ctx *Opfamily_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opclass_purpose.
	VisitOpclass_purpose(ctx *Opclass_purposeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#recheck_.
	VisitRecheck_(ctx *Recheck_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createopfamilystmt.
	VisitCreateopfamilystmt(ctx *CreateopfamilystmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alteropfamilystmt.
	VisitAlteropfamilystmt(ctx *AlteropfamilystmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opclass_drop_list.
	VisitOpclass_drop_list(ctx *Opclass_drop_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opclass_drop.
	VisitOpclass_drop(ctx *Opclass_dropContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dropopclassstmt.
	VisitDropopclassstmt(ctx *DropopclassstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dropopfamilystmt.
	VisitDropopfamilystmt(ctx *DropopfamilystmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dropownedstmt.
	VisitDropownedstmt(ctx *DropownedstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reassignownedstmt.
	VisitReassignownedstmt(ctx *ReassignownedstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dropstmt.
	VisitDropstmt(ctx *DropstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#object_type_any_name.
	VisitObject_type_any_name(ctx *Object_type_any_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#object_type_name.
	VisitObject_type_name(ctx *Object_type_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#drop_type_name.
	VisitDrop_type_name(ctx *Drop_type_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#object_type_name_on_any_name.
	VisitObject_type_name_on_any_name(ctx *Object_type_name_on_any_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#any_name_list_.
	VisitAny_name_list_(ctx *Any_name_list_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#any_name.
	VisitAny_name(ctx *Any_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#attrs.
	VisitAttrs(ctx *AttrsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#type_name_list.
	VisitType_name_list(ctx *Type_name_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#truncatestmt.
	VisitTruncatestmt(ctx *TruncatestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#restart_seqs_.
	VisitRestart_seqs_(ctx *Restart_seqs_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#commentstmt.
	VisitCommentstmt(ctx *CommentstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#comment_text.
	VisitComment_text(ctx *Comment_textContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#seclabelstmt.
	VisitSeclabelstmt(ctx *SeclabelstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#provider_.
	VisitProvider_(ctx *Provider_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#security_label.
	VisitSecurity_label(ctx *Security_labelContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#fetchstmt.
	VisitFetchstmt(ctx *FetchstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#fetch_args.
	VisitFetch_args(ctx *Fetch_argsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#from_in.
	VisitFrom_in(ctx *From_inContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#from_in_.
	VisitFrom_in_(ctx *From_in_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#grantstmt.
	VisitGrantstmt(ctx *GrantstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#revokestmt.
	VisitRevokestmt(ctx *RevokestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#privileges.
	VisitPrivileges(ctx *PrivilegesContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#privilege_list.
	VisitPrivilege_list(ctx *Privilege_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#privilege.
	VisitPrivilege(ctx *PrivilegeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#privilege_target.
	VisitPrivilege_target(ctx *Privilege_targetContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#grantee_list.
	VisitGrantee_list(ctx *Grantee_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#grantee.
	VisitGrantee(ctx *GranteeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#grant_grant_option_.
	VisitGrant_grant_option_(ctx *Grant_grant_option_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#grantrolestmt.
	VisitGrantrolestmt(ctx *GrantrolestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#revokerolestmt.
	VisitRevokerolestmt(ctx *RevokerolestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#grant_admin_option_.
	VisitGrant_admin_option_(ctx *Grant_admin_option_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#granted_by_.
	VisitGranted_by_(ctx *Granted_by_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterdefaultprivilegesstmt.
	VisitAlterdefaultprivilegesstmt(ctx *AlterdefaultprivilegesstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#defacloptionlist.
	VisitDefacloptionlist(ctx *DefacloptionlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#defacloption.
	VisitDefacloption(ctx *DefacloptionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#defaclaction.
	VisitDefaclaction(ctx *DefaclactionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#defacl_privilege_target.
	VisitDefacl_privilege_target(ctx *Defacl_privilege_targetContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#indexstmt.
	VisitIndexstmt(ctx *IndexstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#unique_.
	VisitUnique_(ctx *Unique_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#single_name_.
	VisitSingle_name_(ctx *Single_name_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#concurrently_.
	VisitConcurrently_(ctx *Concurrently_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#index_name_.
	VisitIndex_name_(ctx *Index_name_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#access_method_clause.
	VisitAccess_method_clause(ctx *Access_method_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#index_params.
	VisitIndex_params(ctx *Index_paramsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#index_elem_options.
	VisitIndex_elem_options(ctx *Index_elem_optionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#index_elem.
	VisitIndex_elem(ctx *Index_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#include_.
	VisitInclude_(ctx *Include_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#index_including_params.
	VisitIndex_including_params(ctx *Index_including_paramsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#collate_.
	VisitCollate_(ctx *Collate_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#class_.
	VisitClass_(ctx *Class_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#asc_desc_.
	VisitAsc_desc_(ctx *Asc_desc_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#nulls_order_.
	VisitNulls_order_(ctx *Nulls_order_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createfunctionstmt.
	VisitCreatefunctionstmt(ctx *CreatefunctionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#or_replace_.
	VisitOr_replace_(ctx *Or_replace_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_args.
	VisitFunc_args(ctx *Func_argsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_args_list.
	VisitFunc_args_list(ctx *Func_args_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#function_with_argtypes_list.
	VisitFunction_with_argtypes_list(ctx *Function_with_argtypes_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#function_with_argtypes.
	VisitFunction_with_argtypes(ctx *Function_with_argtypesContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_args_with_defaults.
	VisitFunc_args_with_defaults(ctx *Func_args_with_defaultsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_args_with_defaults_list.
	VisitFunc_args_with_defaults_list(ctx *Func_args_with_defaults_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_arg.
	VisitFunc_arg(ctx *Func_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#arg_class.
	VisitArg_class(ctx *Arg_classContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#param_name.
	VisitParam_name(ctx *Param_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_return.
	VisitFunc_return(ctx *Func_returnContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_type.
	VisitFunc_type(ctx *Func_typeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_arg_with_default.
	VisitFunc_arg_with_default(ctx *Func_arg_with_defaultContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#aggr_arg.
	VisitAggr_arg(ctx *Aggr_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#aggr_args.
	VisitAggr_args(ctx *Aggr_argsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#aggr_args_list.
	VisitAggr_args_list(ctx *Aggr_args_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#aggregate_with_argtypes.
	VisitAggregate_with_argtypes(ctx *Aggregate_with_argtypesContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#aggregate_with_argtypes_list.
	VisitAggregate_with_argtypes_list(ctx *Aggregate_with_argtypes_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createfunc_opt_list.
	VisitCreatefunc_opt_list(ctx *Createfunc_opt_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#common_func_opt_item.
	VisitCommon_func_opt_item(ctx *Common_func_opt_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createfunc_opt_item.
	VisitCreatefunc_opt_item(ctx *Createfunc_opt_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_as.
	VisitFunc_as(ctx *Func_asContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transform_type_list.
	VisitTransform_type_list(ctx *Transform_type_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#definition_.
	VisitDefinition_(ctx *Definition_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#table_func_column.
	VisitTable_func_column(ctx *Table_func_columnContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#table_func_column_list.
	VisitTable_func_column_list(ctx *Table_func_column_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterfunctionstmt.
	VisitAlterfunctionstmt(ctx *AlterfunctionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterfunc_opt_list.
	VisitAlterfunc_opt_list(ctx *Alterfunc_opt_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#restrict_.
	VisitRestrict_(ctx *Restrict_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#removefuncstmt.
	VisitRemovefuncstmt(ctx *RemovefuncstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#removeaggrstmt.
	VisitRemoveaggrstmt(ctx *RemoveaggrstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#removeoperstmt.
	VisitRemoveoperstmt(ctx *RemoveoperstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#oper_argtypes.
	VisitOper_argtypes(ctx *Oper_argtypesContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#any_operator.
	VisitAny_operator(ctx *Any_operatorContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#operator_with_argtypes_list.
	VisitOperator_with_argtypes_list(ctx *Operator_with_argtypes_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#operator_with_argtypes.
	VisitOperator_with_argtypes(ctx *Operator_with_argtypesContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dostmt.
	VisitDostmt(ctx *DostmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dostmt_opt_list.
	VisitDostmt_opt_list(ctx *Dostmt_opt_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dostmt_opt_item.
	VisitDostmt_opt_item(ctx *Dostmt_opt_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createcaststmt.
	VisitCreatecaststmt(ctx *CreatecaststmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#cast_context.
	VisitCast_context(ctx *Cast_contextContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dropcaststmt.
	VisitDropcaststmt(ctx *DropcaststmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#if_exists_.
	VisitIf_exists_(ctx *If_exists_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createtransformstmt.
	VisitCreatetransformstmt(ctx *CreatetransformstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transform_element_list.
	VisitTransform_element_list(ctx *Transform_element_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#droptransformstmt.
	VisitDroptransformstmt(ctx *DroptransformstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reindexstmt.
	VisitReindexstmt(ctx *ReindexstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reindex_target_relation.
	VisitReindex_target_relation(ctx *Reindex_target_relationContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reindex_target_all.
	VisitReindex_target_all(ctx *Reindex_target_allContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reindex_option_list.
	VisitReindex_option_list(ctx *Reindex_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altertblspcstmt.
	VisitAltertblspcstmt(ctx *AltertblspcstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#renamestmt.
	VisitRenamestmt(ctx *RenamestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#column_.
	VisitColumn_(ctx *Column_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#set_data_.
	VisitSet_data_(ctx *Set_data_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterobjectdependsstmt.
	VisitAlterobjectdependsstmt(ctx *AlterobjectdependsstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#no_.
	VisitNo_(ctx *No_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterobjectschemastmt.
	VisitAlterobjectschemastmt(ctx *AlterobjectschemastmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alteroperatorstmt.
	VisitAlteroperatorstmt(ctx *AlteroperatorstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#operator_def_list.
	VisitOperator_def_list(ctx *Operator_def_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#operator_def_elem.
	VisitOperator_def_elem(ctx *Operator_def_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#operator_def_arg.
	VisitOperator_def_arg(ctx *Operator_def_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altertypestmt.
	VisitAltertypestmt(ctx *AltertypestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterownerstmt.
	VisitAlterownerstmt(ctx *AlterownerstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createpublicationstmt.
	VisitCreatepublicationstmt(ctx *CreatepublicationstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#publication_for_tables_.
	VisitPublication_for_tables_(ctx *Publication_for_tables_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#publication_for_tables.
	VisitPublication_for_tables(ctx *Publication_for_tablesContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterpublicationstmt.
	VisitAlterpublicationstmt(ctx *AlterpublicationstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createsubscriptionstmt.
	VisitCreatesubscriptionstmt(ctx *CreatesubscriptionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#publication_name_list.
	VisitPublication_name_list(ctx *Publication_name_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#publication_name_item.
	VisitPublication_name_item(ctx *Publication_name_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altersubscriptionstmt.
	VisitAltersubscriptionstmt(ctx *AltersubscriptionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dropsubscriptionstmt.
	VisitDropsubscriptionstmt(ctx *DropsubscriptionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rulestmt.
	VisitRulestmt(ctx *RulestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#ruleactionlist.
	VisitRuleactionlist(ctx *RuleactionlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#ruleactionmulti.
	VisitRuleactionmulti(ctx *RuleactionmultiContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#ruleactionstmt.
	VisitRuleactionstmt(ctx *RuleactionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#ruleactionstmtOrEmpty.
	VisitRuleactionstmtOrEmpty(ctx *RuleactionstmtOrEmptyContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#event.
	VisitEvent(ctx *EventContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#instead_.
	VisitInstead_(ctx *Instead_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#notifystmt.
	VisitNotifystmt(ctx *NotifystmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#notify_payload.
	VisitNotify_payload(ctx *Notify_payloadContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#listenstmt.
	VisitListenstmt(ctx *ListenstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#unlistenstmt.
	VisitUnlistenstmt(ctx *UnlistenstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transactionstmt.
	VisitTransactionstmt(ctx *TransactionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transaction_.
	VisitTransaction_(ctx *Transaction_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transaction_mode_item.
	VisitTransaction_mode_item(ctx *Transaction_mode_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transaction_mode_list.
	VisitTransaction_mode_list(ctx *Transaction_mode_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transaction_mode_list_or_empty.
	VisitTransaction_mode_list_or_empty(ctx *Transaction_mode_list_or_emptyContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#transaction_chain_.
	VisitTransaction_chain_(ctx *Transaction_chain_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#viewstmt.
	VisitViewstmt(ctx *ViewstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#check_option_.
	VisitCheck_option_(ctx *Check_option_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#loadstmt.
	VisitLoadstmt(ctx *LoadstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createdbstmt.
	VisitCreatedbstmt(ctx *CreatedbstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createdb_opt_list.
	VisitCreatedb_opt_list(ctx *Createdb_opt_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createdb_opt_items.
	VisitCreatedb_opt_items(ctx *Createdb_opt_itemsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createdb_opt_item.
	VisitCreatedb_opt_item(ctx *Createdb_opt_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createdb_opt_name.
	VisitCreatedb_opt_name(ctx *Createdb_opt_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#equal_.
	VisitEqual_(ctx *Equal_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterdatabasestmt.
	VisitAlterdatabasestmt(ctx *AlterdatabasestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterdatabasesetstmt.
	VisitAlterdatabasesetstmt(ctx *AlterdatabasesetstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#dropdbstmt.
	VisitDropdbstmt(ctx *DropdbstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#drop_option_list.
	VisitDrop_option_list(ctx *Drop_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#drop_option.
	VisitDrop_option(ctx *Drop_optionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altercollationstmt.
	VisitAltercollationstmt(ctx *AltercollationstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altersystemstmt.
	VisitAltersystemstmt(ctx *AltersystemstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createdomainstmt.
	VisitCreatedomainstmt(ctx *CreatedomainstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alterdomainstmt.
	VisitAlterdomainstmt(ctx *AlterdomainstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#as_.
	VisitAs_(ctx *As_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altertsdictionarystmt.
	VisitAltertsdictionarystmt(ctx *AltertsdictionarystmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#altertsconfigurationstmt.
	VisitAltertsconfigurationstmt(ctx *AltertsconfigurationstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#any_with.
	VisitAny_with(ctx *Any_withContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#createconversionstmt.
	VisitCreateconversionstmt(ctx *CreateconversionstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#clusterstmt.
	VisitClusterstmt(ctx *ClusterstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#cluster_index_specification.
	VisitCluster_index_specification(ctx *Cluster_index_specificationContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#vacuumstmt.
	VisitVacuumstmt(ctx *VacuumstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#analyzestmt.
	VisitAnalyzestmt(ctx *AnalyzestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#utility_option_list.
	VisitUtility_option_list(ctx *Utility_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#vac_analyze_option_list.
	VisitVac_analyze_option_list(ctx *Vac_analyze_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#analyze_keyword.
	VisitAnalyze_keyword(ctx *Analyze_keywordContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#utility_option_elem.
	VisitUtility_option_elem(ctx *Utility_option_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#utility_option_name.
	VisitUtility_option_name(ctx *Utility_option_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#utility_option_arg.
	VisitUtility_option_arg(ctx *Utility_option_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#vac_analyze_option_elem.
	VisitVac_analyze_option_elem(ctx *Vac_analyze_option_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#vac_analyze_option_name.
	VisitVac_analyze_option_name(ctx *Vac_analyze_option_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#vac_analyze_option_arg.
	VisitVac_analyze_option_arg(ctx *Vac_analyze_option_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#analyze_.
	VisitAnalyze_(ctx *Analyze_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#verbose_.
	VisitVerbose_(ctx *Verbose_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#full_.
	VisitFull_(ctx *Full_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#freeze_.
	VisitFreeze_(ctx *Freeze_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#name_list_.
	VisitName_list_(ctx *Name_list_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#vacuum_relation.
	VisitVacuum_relation(ctx *Vacuum_relationContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#vacuum_relation_list.
	VisitVacuum_relation_list(ctx *Vacuum_relation_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#vacuum_relation_list_.
	VisitVacuum_relation_list_(ctx *Vacuum_relation_list_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#explainstmt.
	VisitExplainstmt(ctx *ExplainstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#explainablestmt.
	VisitExplainablestmt(ctx *ExplainablestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#explain_option_list.
	VisitExplain_option_list(ctx *Explain_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#explain_option_elem.
	VisitExplain_option_elem(ctx *Explain_option_elemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#explain_option_name.
	VisitExplain_option_name(ctx *Explain_option_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#explain_option_arg.
	VisitExplain_option_arg(ctx *Explain_option_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#preparestmt.
	VisitPreparestmt(ctx *PreparestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#prep_type_clause.
	VisitPrep_type_clause(ctx *Prep_type_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#preparablestmt.
	VisitPreparablestmt(ctx *PreparablestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#executestmt.
	VisitExecutestmt(ctx *ExecutestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#execute_param_clause.
	VisitExecute_param_clause(ctx *Execute_param_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#deallocatestmt.
	VisitDeallocatestmt(ctx *DeallocatestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#insertstmt.
	VisitInsertstmt(ctx *InsertstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#insert_target.
	VisitInsert_target(ctx *Insert_targetContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#insert_rest.
	VisitInsert_rest(ctx *Insert_restContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#override_kind.
	VisitOverride_kind(ctx *Override_kindContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#insert_column_list.
	VisitInsert_column_list(ctx *Insert_column_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#insert_column_item.
	VisitInsert_column_item(ctx *Insert_column_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#on_conflict_.
	VisitOn_conflict_(ctx *On_conflict_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#conf_expr_.
	VisitConf_expr_(ctx *Conf_expr_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#returning_clause.
	VisitReturning_clause(ctx *Returning_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#mergestmt.
	VisitMergestmt(ctx *MergestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#merge_insert_clause.
	VisitMerge_insert_clause(ctx *Merge_insert_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#merge_update_clause.
	VisitMerge_update_clause(ctx *Merge_update_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#merge_delete_clause.
	VisitMerge_delete_clause(ctx *Merge_delete_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#deletestmt.
	VisitDeletestmt(ctx *DeletestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#using_clause.
	VisitUsing_clause(ctx *Using_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#lockstmt.
	VisitLockstmt(ctx *LockstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#lock_.
	VisitLock_(ctx *Lock_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#lock_type.
	VisitLock_type(ctx *Lock_typeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#nowait_.
	VisitNowait_(ctx *Nowait_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#nowait_or_skip_.
	VisitNowait_or_skip_(ctx *Nowait_or_skip_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#updatestmt.
	VisitUpdatestmt(ctx *UpdatestmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#set_clause_list.
	VisitSet_clause_list(ctx *Set_clause_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#set_clause.
	VisitSet_clause(ctx *Set_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#set_target.
	VisitSet_target(ctx *Set_targetContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#set_target_list.
	VisitSet_target_list(ctx *Set_target_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#declarecursorstmt.
	VisitDeclarecursorstmt(ctx *DeclarecursorstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#cursor_name.
	VisitCursor_name(ctx *Cursor_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#cursor_options.
	VisitCursor_options(ctx *Cursor_optionsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#hold_.
	VisitHold_(ctx *Hold_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#selectstmt.
	VisitSelectstmt(ctx *SelectstmtContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#select_with_parens.
	VisitSelect_with_parens(ctx *Select_with_parensContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#select_no_parens.
	VisitSelect_no_parens(ctx *Select_no_parensContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#select_clause.
	VisitSelect_clause(ctx *Select_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#simple_select_intersect.
	VisitSimple_select_intersect(ctx *Simple_select_intersectContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#simple_select_pramary.
	VisitSimple_select_pramary(ctx *Simple_select_pramaryContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#with_clause.
	VisitWith_clause(ctx *With_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#cte_list.
	VisitCte_list(ctx *Cte_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#common_table_expr.
	VisitCommon_table_expr(ctx *Common_table_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#materialized_.
	VisitMaterialized_(ctx *Materialized_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#with_clause_.
	VisitWith_clause_(ctx *With_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#into_clause.
	VisitInto_clause(ctx *Into_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#strict_.
	VisitStrict_(ctx *Strict_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opttempTableName.
	VisitOpttempTableName(ctx *OpttempTableNameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#table_.
	VisitTable_(ctx *Table_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#all_or_distinct.
	VisitAll_or_distinct(ctx *All_or_distinctContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#distinct_clause.
	VisitDistinct_clause(ctx *Distinct_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#all_clause_.
	VisitAll_clause_(ctx *All_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#sort_clause_.
	VisitSort_clause_(ctx *Sort_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#sort_clause.
	VisitSort_clause(ctx *Sort_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#sortby_list.
	VisitSortby_list(ctx *Sortby_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#sortby.
	VisitSortby(ctx *SortbyContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#select_limit.
	VisitSelect_limit(ctx *Select_limitContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#select_limit_.
	VisitSelect_limit_(ctx *Select_limit_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#limit_clause.
	VisitLimit_clause(ctx *Limit_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#offset_clause.
	VisitOffset_clause(ctx *Offset_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#select_limit_value.
	VisitSelect_limit_value(ctx *Select_limit_valueContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#select_offset_value.
	VisitSelect_offset_value(ctx *Select_offset_valueContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#select_fetch_first_value.
	VisitSelect_fetch_first_value(ctx *Select_fetch_first_valueContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#i_or_f_const.
	VisitI_or_f_const(ctx *I_or_f_constContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#row_or_rows.
	VisitRow_or_rows(ctx *Row_or_rowsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#first_or_next.
	VisitFirst_or_next(ctx *First_or_nextContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#group_clause.
	VisitGroup_clause(ctx *Group_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#group_by_list.
	VisitGroup_by_list(ctx *Group_by_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#group_by_item.
	VisitGroup_by_item(ctx *Group_by_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#empty_grouping_set.
	VisitEmpty_grouping_set(ctx *Empty_grouping_setContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rollup_clause.
	VisitRollup_clause(ctx *Rollup_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#cube_clause.
	VisitCube_clause(ctx *Cube_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#grouping_sets_clause.
	VisitGrouping_sets_clause(ctx *Grouping_sets_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#having_clause.
	VisitHaving_clause(ctx *Having_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#for_locking_clause.
	VisitFor_locking_clause(ctx *For_locking_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#for_locking_clause_.
	VisitFor_locking_clause_(ctx *For_locking_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#for_locking_items.
	VisitFor_locking_items(ctx *For_locking_itemsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#for_locking_item.
	VisitFor_locking_item(ctx *For_locking_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#for_locking_strength.
	VisitFor_locking_strength(ctx *For_locking_strengthContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#locked_rels_list.
	VisitLocked_rels_list(ctx *Locked_rels_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#values_clause.
	VisitValues_clause(ctx *Values_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#from_clause.
	VisitFrom_clause(ctx *From_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#from_list.
	VisitFrom_list(ctx *From_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#table_ref.
	VisitTable_ref(ctx *Table_refContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#alias_clause.
	VisitAlias_clause(ctx *Alias_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_alias_clause.
	VisitFunc_alias_clause(ctx *Func_alias_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#join_type.
	VisitJoin_type(ctx *Join_typeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#join_qual.
	VisitJoin_qual(ctx *Join_qualContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#relation_expr.
	VisitRelation_expr(ctx *Relation_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#relation_expr_list.
	VisitRelation_expr_list(ctx *Relation_expr_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#relation_expr_opt_alias.
	VisitRelation_expr_opt_alias(ctx *Relation_expr_opt_aliasContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tablesample_clause.
	VisitTablesample_clause(ctx *Tablesample_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#repeatable_clause_.
	VisitRepeatable_clause_(ctx *Repeatable_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_table.
	VisitFunc_table(ctx *Func_tableContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rowsfrom_item.
	VisitRowsfrom_item(ctx *Rowsfrom_itemContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rowsfrom_list.
	VisitRowsfrom_list(ctx *Rowsfrom_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#col_def_list_.
	VisitCol_def_list_(ctx *Col_def_list_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#ordinality_.
	VisitOrdinality_(ctx *Ordinality_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#where_clause.
	VisitWhere_clause(ctx *Where_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#where_or_current_clause.
	VisitWhere_or_current_clause(ctx *Where_or_current_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opttablefuncelementlist.
	VisitOpttablefuncelementlist(ctx *OpttablefuncelementlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tablefuncelementlist.
	VisitTablefuncelementlist(ctx *TablefuncelementlistContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#tablefuncelement.
	VisitTablefuncelement(ctx *TablefuncelementContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xmltable.
	VisitXmltable(ctx *XmltableContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xmltable_column_list.
	VisitXmltable_column_list(ctx *Xmltable_column_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xmltable_column_el.
	VisitXmltable_column_el(ctx *Xmltable_column_elContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xmltable_column_option_list.
	VisitXmltable_column_option_list(ctx *Xmltable_column_option_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xmltable_column_option_el.
	VisitXmltable_column_option_el(ctx *Xmltable_column_option_elContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_namespace_list.
	VisitXml_namespace_list(ctx *Xml_namespace_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_namespace_el.
	VisitXml_namespace_el(ctx *Xml_namespace_elContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#typename.
	VisitTypename(ctx *TypenameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opt_array_bounds.
	VisitOpt_array_bounds(ctx *Opt_array_boundsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#simpletypename.
	VisitSimpletypename(ctx *SimpletypenameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#consttypename.
	VisitConsttypename(ctx *ConsttypenameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#generictype.
	VisitGenerictype(ctx *GenerictypeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#type_modifiers_.
	VisitType_modifiers_(ctx *Type_modifiers_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#numeric.
	VisitNumeric(ctx *NumericContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#float_.
	VisitFloat_(ctx *Float_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#bit.
	VisitBit(ctx *BitContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constbit.
	VisitConstbit(ctx *ConstbitContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#bitwithlength.
	VisitBitwithlength(ctx *BitwithlengthContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#bitwithoutlength.
	VisitBitwithoutlength(ctx *BitwithoutlengthContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#character.
	VisitCharacter(ctx *CharacterContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constcharacter.
	VisitConstcharacter(ctx *ConstcharacterContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#character_c.
	VisitCharacter_c(ctx *Character_cContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#varying_.
	VisitVarying_(ctx *Varying_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constdatetime.
	VisitConstdatetime(ctx *ConstdatetimeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#constinterval.
	VisitConstinterval(ctx *ConstintervalContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#timezone_.
	VisitTimezone_(ctx *Timezone_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#interval_.
	VisitInterval_(ctx *Interval_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#interval_second.
	VisitInterval_second(ctx *Interval_secondContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#jsonType.
	VisitJsonType(ctx *JsonTypeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#escape_.
	VisitEscape_(ctx *Escape_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr.
	VisitA_expr(ctx *A_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_qual.
	VisitA_expr_qual(ctx *A_expr_qualContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_lessless.
	VisitA_expr_lessless(ctx *A_expr_lesslessContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_or.
	VisitA_expr_or(ctx *A_expr_orContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_and.
	VisitA_expr_and(ctx *A_expr_andContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_between.
	VisitA_expr_between(ctx *A_expr_betweenContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_in.
	VisitA_expr_in(ctx *A_expr_inContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_unary_not.
	VisitA_expr_unary_not(ctx *A_expr_unary_notContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_isnull.
	VisitA_expr_isnull(ctx *A_expr_isnullContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_is_not.
	VisitA_expr_is_not(ctx *A_expr_is_notContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_compare.
	VisitA_expr_compare(ctx *A_expr_compareContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_like.
	VisitA_expr_like(ctx *A_expr_likeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_qual_op.
	VisitA_expr_qual_op(ctx *A_expr_qual_opContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_unary_qualop.
	VisitA_expr_unary_qualop(ctx *A_expr_unary_qualopContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_add.
	VisitA_expr_add(ctx *A_expr_addContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_mul.
	VisitA_expr_mul(ctx *A_expr_mulContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_caret.
	VisitA_expr_caret(ctx *A_expr_caretContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_unary_sign.
	VisitA_expr_unary_sign(ctx *A_expr_unary_signContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_at_time_zone.
	VisitA_expr_at_time_zone(ctx *A_expr_at_time_zoneContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_collate.
	VisitA_expr_collate(ctx *A_expr_collateContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#a_expr_typecast.
	VisitA_expr_typecast(ctx *A_expr_typecastContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#b_expr.
	VisitB_expr(ctx *B_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#c_expr_exists.
	VisitC_expr_exists(ctx *C_expr_existsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#c_expr_expr.
	VisitC_expr_expr(ctx *C_expr_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#c_expr_case.
	VisitC_expr_case(ctx *C_expr_caseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#plsqlvariablename.
	VisitPlsqlvariablename(ctx *PlsqlvariablenameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_application.
	VisitFunc_application(ctx *Func_applicationContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_expr.
	VisitFunc_expr(ctx *Func_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_expr_windowless.
	VisitFunc_expr_windowless(ctx *Func_expr_windowlessContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_expr_common_subexpr.
	VisitFunc_expr_common_subexpr(ctx *Func_expr_common_subexprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_root_version.
	VisitXml_root_version(ctx *Xml_root_versionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_root_standalone_.
	VisitXml_root_standalone_(ctx *Xml_root_standalone_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_attributes.
	VisitXml_attributes(ctx *Xml_attributesContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_attribute_list.
	VisitXml_attribute_list(ctx *Xml_attribute_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_attribute_el.
	VisitXml_attribute_el(ctx *Xml_attribute_elContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#document_or_content.
	VisitDocument_or_content(ctx *Document_or_contentContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_whitespace_option.
	VisitXml_whitespace_option(ctx *Xml_whitespace_optionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xmlexists_argument.
	VisitXmlexists_argument(ctx *Xmlexists_argumentContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xml_passing_mech.
	VisitXml_passing_mech(ctx *Xml_passing_mechContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#within_group_clause.
	VisitWithin_group_clause(ctx *Within_group_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#filter_clause.
	VisitFilter_clause(ctx *Filter_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#window_clause.
	VisitWindow_clause(ctx *Window_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#window_definition_list.
	VisitWindow_definition_list(ctx *Window_definition_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#window_definition.
	VisitWindow_definition(ctx *Window_definitionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#over_clause.
	VisitOver_clause(ctx *Over_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#window_specification.
	VisitWindow_specification(ctx *Window_specificationContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#existing_window_name_.
	VisitExisting_window_name_(ctx *Existing_window_name_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#partition_clause_.
	VisitPartition_clause_(ctx *Partition_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#frame_clause_.
	VisitFrame_clause_(ctx *Frame_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#frame_extent.
	VisitFrame_extent(ctx *Frame_extentContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#frame_bound.
	VisitFrame_bound(ctx *Frame_boundContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#window_exclusion_clause_.
	VisitWindow_exclusion_clause_(ctx *Window_exclusion_clause_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#row.
	VisitRow(ctx *RowContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#explicit_row.
	VisitExplicit_row(ctx *Explicit_rowContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#implicit_row.
	VisitImplicit_row(ctx *Implicit_rowContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#sub_type.
	VisitSub_type(ctx *Sub_typeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#all_op.
	VisitAll_op(ctx *All_opContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#mathop.
	VisitMathop(ctx *MathopContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#qual_op.
	VisitQual_op(ctx *Qual_opContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#qual_all_op.
	VisitQual_all_op(ctx *Qual_all_opContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#subquery_Op.
	VisitSubquery_Op(ctx *Subquery_OpContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#expr_list.
	VisitExpr_list(ctx *Expr_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_arg_list.
	VisitFunc_arg_list(ctx *Func_arg_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_arg_expr.
	VisitFunc_arg_expr(ctx *Func_arg_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#type_list.
	VisitType_list(ctx *Type_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#array_expr.
	VisitArray_expr(ctx *Array_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#array_expr_list.
	VisitArray_expr_list(ctx *Array_expr_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#extract_list.
	VisitExtract_list(ctx *Extract_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#extract_arg.
	VisitExtract_arg(ctx *Extract_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#unicode_normal_form.
	VisitUnicode_normal_form(ctx *Unicode_normal_formContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#overlay_list.
	VisitOverlay_list(ctx *Overlay_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#position_list.
	VisitPosition_list(ctx *Position_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#substr_list.
	VisitSubstr_list(ctx *Substr_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#trim_list.
	VisitTrim_list(ctx *Trim_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#in_expr_select.
	VisitIn_expr_select(ctx *In_expr_selectContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#in_expr_list.
	VisitIn_expr_list(ctx *In_expr_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#case_expr.
	VisitCase_expr(ctx *Case_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#when_clause_list.
	VisitWhen_clause_list(ctx *When_clause_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#when_clause.
	VisitWhen_clause(ctx *When_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#case_default.
	VisitCase_default(ctx *Case_defaultContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#case_arg.
	VisitCase_arg(ctx *Case_argContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#columnref.
	VisitColumnref(ctx *ColumnrefContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#indirection_el.
	VisitIndirection_el(ctx *Indirection_elContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#slice_bound_.
	VisitSlice_bound_(ctx *Slice_bound_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#indirection.
	VisitIndirection(ctx *IndirectionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#opt_indirection.
	VisitOpt_indirection(ctx *Opt_indirectionContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_passing_clause.
	VisitJson_passing_clause(ctx *Json_passing_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_arguments.
	VisitJson_arguments(ctx *Json_argumentsContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_argument.
	VisitJson_argument(ctx *Json_argumentContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_wrapper_behavior.
	VisitJson_wrapper_behavior(ctx *Json_wrapper_behaviorContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_behavior.
	VisitJson_behavior(ctx *Json_behaviorContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_behavior_type.
	VisitJson_behavior_type(ctx *Json_behavior_typeContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_behavior_clause.
	VisitJson_behavior_clause(ctx *Json_behavior_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_on_error_clause.
	VisitJson_on_error_clause(ctx *Json_on_error_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_value_expr.
	VisitJson_value_expr(ctx *Json_value_exprContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_format_clause.
	VisitJson_format_clause(ctx *Json_format_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_quotes_clause.
	VisitJson_quotes_clause(ctx *Json_quotes_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_returning_clause.
	VisitJson_returning_clause(ctx *Json_returning_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_predicate_type_constraint.
	VisitJson_predicate_type_constraint(ctx *Json_predicate_type_constraintContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_key_uniqueness_constraint.
	VisitJson_key_uniqueness_constraint(ctx *Json_key_uniqueness_constraintContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_name_and_value_list.
	VisitJson_name_and_value_list(ctx *Json_name_and_value_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_name_and_value.
	VisitJson_name_and_value(ctx *Json_name_and_valueContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_object_constructor_null_clause.
	VisitJson_object_constructor_null_clause(ctx *Json_object_constructor_null_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_array_constructor_null_clause.
	VisitJson_array_constructor_null_clause(ctx *Json_array_constructor_null_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_value_expr_list.
	VisitJson_value_expr_list(ctx *Json_value_expr_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_aggregate_func.
	VisitJson_aggregate_func(ctx *Json_aggregate_funcContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#json_array_aggregate_order_by_clause.
	VisitJson_array_aggregate_order_by_clause(ctx *Json_array_aggregate_order_by_clauseContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#target_list_.
	VisitTarget_list_(ctx *Target_list_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#target_list.
	VisitTarget_list(ctx *Target_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#target_label.
	VisitTarget_label(ctx *Target_labelContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#target_star.
	VisitTarget_star(ctx *Target_starContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#qualified_name_list.
	VisitQualified_name_list(ctx *Qualified_name_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#qualified_name.
	VisitQualified_name(ctx *Qualified_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#name_list.
	VisitName_list(ctx *Name_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#name.
	VisitName(ctx *NameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#attr_name.
	VisitAttr_name(ctx *Attr_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#file_name.
	VisitFile_name(ctx *File_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#func_name.
	VisitFunc_name(ctx *Func_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#aexprconst.
	VisitAexprconst(ctx *AexprconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#xconst.
	VisitXconst(ctx *XconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#bconst.
	VisitBconst(ctx *BconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#fconst.
	VisitFconst(ctx *FconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#iconst.
	VisitIconst(ctx *IconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#sconst.
	VisitSconst(ctx *SconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#anysconst.
	VisitAnysconst(ctx *AnysconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#uescape_.
	VisitUescape_(ctx *Uescape_Context) interface{}

	// Visit a parse tree produced by PostgreSQLParser#signediconst.
	VisitSignediconst(ctx *SignediconstContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#roleid.
	VisitRoleid(ctx *RoleidContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#rolespec.
	VisitRolespec(ctx *RolespecContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#role_list.
	VisitRole_list(ctx *Role_listContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#colid.
	VisitColid(ctx *ColidContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#type_function_name.
	VisitType_function_name(ctx *Type_function_nameContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#nonreservedword.
	VisitNonreservedword(ctx *NonreservedwordContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#colLabel.
	VisitColLabel(ctx *ColLabelContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#bareColLabel.
	VisitBareColLabel(ctx *BareColLabelContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#unreserved_keyword.
	VisitUnreserved_keyword(ctx *Unreserved_keywordContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#col_name_keyword.
	VisitCol_name_keyword(ctx *Col_name_keywordContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#type_func_name_keyword.
	VisitType_func_name_keyword(ctx *Type_func_name_keywordContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#reserved_keyword.
	VisitReserved_keyword(ctx *Reserved_keywordContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#bare_label_keyword.
	VisitBare_label_keyword(ctx *Bare_label_keywordContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#any_identifier.
	VisitAny_identifier(ctx *Any_identifierContext) interface{}

	// Visit a parse tree produced by PostgreSQLParser#identifier.
	VisitIdentifier(ctx *IdentifierContext) interface{}
}
