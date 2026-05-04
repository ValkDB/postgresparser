// Code generated from PostgreSQLParser.g4 by ANTLR 4.13.1. DO NOT EDIT.

package gen // PostgreSQLParser
import "github.com/antlr4-go/antlr/v4"

// PostgreSQLParserListener is a complete listener for a parse tree produced by PostgreSQLParser.
type PostgreSQLParserListener interface {
	antlr.ParseTreeListener

	// EnterRoot is called when entering the root production.
	EnterRoot(c *RootContext)

	// EnterStmtblock is called when entering the stmtblock production.
	EnterStmtblock(c *StmtblockContext)

	// EnterStmtmulti is called when entering the stmtmulti production.
	EnterStmtmulti(c *StmtmultiContext)

	// EnterStmt is called when entering the stmt production.
	EnterStmt(c *StmtContext)

	// EnterCallstmt is called when entering the callstmt production.
	EnterCallstmt(c *CallstmtContext)

	// EnterCreaterolestmt is called when entering the createrolestmt production.
	EnterCreaterolestmt(c *CreaterolestmtContext)

	// EnterWith_ is called when entering the with_ production.
	EnterWith_(c *With_Context)

	// EnterOptrolelist is called when entering the optrolelist production.
	EnterOptrolelist(c *OptrolelistContext)

	// EnterAlteroptrolelist is called when entering the alteroptrolelist production.
	EnterAlteroptrolelist(c *AlteroptrolelistContext)

	// EnterAlteroptroleelem is called when entering the alteroptroleelem production.
	EnterAlteroptroleelem(c *AlteroptroleelemContext)

	// EnterCreateoptroleelem is called when entering the createoptroleelem production.
	EnterCreateoptroleelem(c *CreateoptroleelemContext)

	// EnterCreateuserstmt is called when entering the createuserstmt production.
	EnterCreateuserstmt(c *CreateuserstmtContext)

	// EnterAlterrolestmt is called when entering the alterrolestmt production.
	EnterAlterrolestmt(c *AlterrolestmtContext)

	// EnterIn_database_ is called when entering the in_database_ production.
	EnterIn_database_(c *In_database_Context)

	// EnterAlterrolesetstmt is called when entering the alterrolesetstmt production.
	EnterAlterrolesetstmt(c *AlterrolesetstmtContext)

	// EnterDroprolestmt is called when entering the droprolestmt production.
	EnterDroprolestmt(c *DroprolestmtContext)

	// EnterCreategroupstmt is called when entering the creategroupstmt production.
	EnterCreategroupstmt(c *CreategroupstmtContext)

	// EnterAltergroupstmt is called when entering the altergroupstmt production.
	EnterAltergroupstmt(c *AltergroupstmtContext)

	// EnterAdd_drop is called when entering the add_drop production.
	EnterAdd_drop(c *Add_dropContext)

	// EnterCreateschemastmt is called when entering the createschemastmt production.
	EnterCreateschemastmt(c *CreateschemastmtContext)

	// EnterOptschemaname is called when entering the optschemaname production.
	EnterOptschemaname(c *OptschemanameContext)

	// EnterOptschemaeltlist is called when entering the optschemaeltlist production.
	EnterOptschemaeltlist(c *OptschemaeltlistContext)

	// EnterSchema_stmt is called when entering the schema_stmt production.
	EnterSchema_stmt(c *Schema_stmtContext)

	// EnterVariablesetstmt is called when entering the variablesetstmt production.
	EnterVariablesetstmt(c *VariablesetstmtContext)

	// EnterSet_rest is called when entering the set_rest production.
	EnterSet_rest(c *Set_restContext)

	// EnterGeneric_set is called when entering the generic_set production.
	EnterGeneric_set(c *Generic_setContext)

	// EnterSet_rest_more is called when entering the set_rest_more production.
	EnterSet_rest_more(c *Set_rest_moreContext)

	// EnterVar_name is called when entering the var_name production.
	EnterVar_name(c *Var_nameContext)

	// EnterVar_list is called when entering the var_list production.
	EnterVar_list(c *Var_listContext)

	// EnterVar_value is called when entering the var_value production.
	EnterVar_value(c *Var_valueContext)

	// EnterIso_level is called when entering the iso_level production.
	EnterIso_level(c *Iso_levelContext)

	// EnterBoolean_or_string_ is called when entering the boolean_or_string_ production.
	EnterBoolean_or_string_(c *Boolean_or_string_Context)

	// EnterZone_value is called when entering the zone_value production.
	EnterZone_value(c *Zone_valueContext)

	// EnterEncoding_ is called when entering the encoding_ production.
	EnterEncoding_(c *Encoding_Context)

	// EnterNonreservedword_or_sconst is called when entering the nonreservedword_or_sconst production.
	EnterNonreservedword_or_sconst(c *Nonreservedword_or_sconstContext)

	// EnterVariableresetstmt is called when entering the variableresetstmt production.
	EnterVariableresetstmt(c *VariableresetstmtContext)

	// EnterReset_rest is called when entering the reset_rest production.
	EnterReset_rest(c *Reset_restContext)

	// EnterGeneric_reset is called when entering the generic_reset production.
	EnterGeneric_reset(c *Generic_resetContext)

	// EnterSetresetclause is called when entering the setresetclause production.
	EnterSetresetclause(c *SetresetclauseContext)

	// EnterFunctionsetresetclause is called when entering the functionsetresetclause production.
	EnterFunctionsetresetclause(c *FunctionsetresetclauseContext)

	// EnterVariableshowstmt is called when entering the variableshowstmt production.
	EnterVariableshowstmt(c *VariableshowstmtContext)

	// EnterConstraintssetstmt is called when entering the constraintssetstmt production.
	EnterConstraintssetstmt(c *ConstraintssetstmtContext)

	// EnterConstraints_set_list is called when entering the constraints_set_list production.
	EnterConstraints_set_list(c *Constraints_set_listContext)

	// EnterConstraints_set_mode is called when entering the constraints_set_mode production.
	EnterConstraints_set_mode(c *Constraints_set_modeContext)

	// EnterCheckpointstmt is called when entering the checkpointstmt production.
	EnterCheckpointstmt(c *CheckpointstmtContext)

	// EnterDiscardstmt is called when entering the discardstmt production.
	EnterDiscardstmt(c *DiscardstmtContext)

	// EnterAltertablestmt is called when entering the altertablestmt production.
	EnterAltertablestmt(c *AltertablestmtContext)

	// EnterAlter_table_cmds is called when entering the alter_table_cmds production.
	EnterAlter_table_cmds(c *Alter_table_cmdsContext)

	// EnterPartition_cmd is called when entering the partition_cmd production.
	EnterPartition_cmd(c *Partition_cmdContext)

	// EnterIndex_partition_cmd is called when entering the index_partition_cmd production.
	EnterIndex_partition_cmd(c *Index_partition_cmdContext)

	// EnterAlter_table_cmd is called when entering the alter_table_cmd production.
	EnterAlter_table_cmd(c *Alter_table_cmdContext)

	// EnterAlter_column_default is called when entering the alter_column_default production.
	EnterAlter_column_default(c *Alter_column_defaultContext)

	// EnterDrop_behavior_ is called when entering the drop_behavior_ production.
	EnterDrop_behavior_(c *Drop_behavior_Context)

	// EnterCollate_clause_ is called when entering the collate_clause_ production.
	EnterCollate_clause_(c *Collate_clause_Context)

	// EnterAlter_using is called when entering the alter_using production.
	EnterAlter_using(c *Alter_usingContext)

	// EnterReplica_identity is called when entering the replica_identity production.
	EnterReplica_identity(c *Replica_identityContext)

	// EnterReloptions is called when entering the reloptions production.
	EnterReloptions(c *ReloptionsContext)

	// EnterReloptions_ is called when entering the reloptions_ production.
	EnterReloptions_(c *Reloptions_Context)

	// EnterReloption_list is called when entering the reloption_list production.
	EnterReloption_list(c *Reloption_listContext)

	// EnterReloption_elem is called when entering the reloption_elem production.
	EnterReloption_elem(c *Reloption_elemContext)

	// EnterAlter_identity_column_option_list is called when entering the alter_identity_column_option_list production.
	EnterAlter_identity_column_option_list(c *Alter_identity_column_option_listContext)

	// EnterAlter_identity_column_option is called when entering the alter_identity_column_option production.
	EnterAlter_identity_column_option(c *Alter_identity_column_optionContext)

	// EnterPartitionboundspec is called when entering the partitionboundspec production.
	EnterPartitionboundspec(c *PartitionboundspecContext)

	// EnterHash_partbound_elem is called when entering the hash_partbound_elem production.
	EnterHash_partbound_elem(c *Hash_partbound_elemContext)

	// EnterHash_partbound is called when entering the hash_partbound production.
	EnterHash_partbound(c *Hash_partboundContext)

	// EnterAltercompositetypestmt is called when entering the altercompositetypestmt production.
	EnterAltercompositetypestmt(c *AltercompositetypestmtContext)

	// EnterAlter_type_cmds is called when entering the alter_type_cmds production.
	EnterAlter_type_cmds(c *Alter_type_cmdsContext)

	// EnterAlter_type_cmd is called when entering the alter_type_cmd production.
	EnterAlter_type_cmd(c *Alter_type_cmdContext)

	// EnterCloseportalstmt is called when entering the closeportalstmt production.
	EnterCloseportalstmt(c *CloseportalstmtContext)

	// EnterCopystmt is called when entering the copystmt production.
	EnterCopystmt(c *CopystmtContext)

	// EnterCopy_from is called when entering the copy_from production.
	EnterCopy_from(c *Copy_fromContext)

	// EnterProgram_ is called when entering the program_ production.
	EnterProgram_(c *Program_Context)

	// EnterCopy_file_name is called when entering the copy_file_name production.
	EnterCopy_file_name(c *Copy_file_nameContext)

	// EnterCopy_options is called when entering the copy_options production.
	EnterCopy_options(c *Copy_optionsContext)

	// EnterCopy_opt_list is called when entering the copy_opt_list production.
	EnterCopy_opt_list(c *Copy_opt_listContext)

	// EnterCopy_opt_item is called when entering the copy_opt_item production.
	EnterCopy_opt_item(c *Copy_opt_itemContext)

	// EnterBinary_ is called when entering the binary_ production.
	EnterBinary_(c *Binary_Context)

	// EnterCopy_delimiter is called when entering the copy_delimiter production.
	EnterCopy_delimiter(c *Copy_delimiterContext)

	// EnterUsing_ is called when entering the using_ production.
	EnterUsing_(c *Using_Context)

	// EnterCopy_generic_opt_list is called when entering the copy_generic_opt_list production.
	EnterCopy_generic_opt_list(c *Copy_generic_opt_listContext)

	// EnterCopy_generic_opt_elem is called when entering the copy_generic_opt_elem production.
	EnterCopy_generic_opt_elem(c *Copy_generic_opt_elemContext)

	// EnterCopy_generic_opt_arg is called when entering the copy_generic_opt_arg production.
	EnterCopy_generic_opt_arg(c *Copy_generic_opt_argContext)

	// EnterCopy_generic_opt_arg_list is called when entering the copy_generic_opt_arg_list production.
	EnterCopy_generic_opt_arg_list(c *Copy_generic_opt_arg_listContext)

	// EnterCopy_generic_opt_arg_list_item is called when entering the copy_generic_opt_arg_list_item production.
	EnterCopy_generic_opt_arg_list_item(c *Copy_generic_opt_arg_list_itemContext)

	// EnterCreatestmt is called when entering the createstmt production.
	EnterCreatestmt(c *CreatestmtContext)

	// EnterOpttemp is called when entering the opttemp production.
	EnterOpttemp(c *OpttempContext)

	// EnterOpttableelementlist is called when entering the opttableelementlist production.
	EnterOpttableelementlist(c *OpttableelementlistContext)

	// EnterOpttypedtableelementlist is called when entering the opttypedtableelementlist production.
	EnterOpttypedtableelementlist(c *OpttypedtableelementlistContext)

	// EnterTableelementlist is called when entering the tableelementlist production.
	EnterTableelementlist(c *TableelementlistContext)

	// EnterTypedtableelementlist is called when entering the typedtableelementlist production.
	EnterTypedtableelementlist(c *TypedtableelementlistContext)

	// EnterTableelement is called when entering the tableelement production.
	EnterTableelement(c *TableelementContext)

	// EnterTypedtableelement is called when entering the typedtableelement production.
	EnterTypedtableelement(c *TypedtableelementContext)

	// EnterColumnDef is called when entering the columnDef production.
	EnterColumnDef(c *ColumnDefContext)

	// EnterColumnOptions is called when entering the columnOptions production.
	EnterColumnOptions(c *ColumnOptionsContext)

	// EnterColquallist is called when entering the colquallist production.
	EnterColquallist(c *ColquallistContext)

	// EnterColconstraint is called when entering the colconstraint production.
	EnterColconstraint(c *ColconstraintContext)

	// EnterColconstraintelem is called when entering the colconstraintelem production.
	EnterColconstraintelem(c *ColconstraintelemContext)

	// EnterGenerated_when is called when entering the generated_when production.
	EnterGenerated_when(c *Generated_whenContext)

	// EnterConstraintattr is called when entering the constraintattr production.
	EnterConstraintattr(c *ConstraintattrContext)

	// EnterTablelikeclause is called when entering the tablelikeclause production.
	EnterTablelikeclause(c *TablelikeclauseContext)

	// EnterTablelikeoptionlist is called when entering the tablelikeoptionlist production.
	EnterTablelikeoptionlist(c *TablelikeoptionlistContext)

	// EnterTablelikeoption is called when entering the tablelikeoption production.
	EnterTablelikeoption(c *TablelikeoptionContext)

	// EnterTableconstraint is called when entering the tableconstraint production.
	EnterTableconstraint(c *TableconstraintContext)

	// EnterConstraintelem is called when entering the constraintelem production.
	EnterConstraintelem(c *ConstraintelemContext)

	// EnterNo_inherit_ is called when entering the no_inherit_ production.
	EnterNo_inherit_(c *No_inherit_Context)

	// EnterColumn_list_ is called when entering the column_list_ production.
	EnterColumn_list_(c *Column_list_Context)

	// EnterColumnlist is called when entering the columnlist production.
	EnterColumnlist(c *ColumnlistContext)

	// EnterColumnElem is called when entering the columnElem production.
	EnterColumnElem(c *ColumnElemContext)

	// EnterC_include_ is called when entering the c_include_ production.
	EnterC_include_(c *C_include_Context)

	// EnterKey_match is called when entering the key_match production.
	EnterKey_match(c *Key_matchContext)

	// EnterExclusionconstraintlist is called when entering the exclusionconstraintlist production.
	EnterExclusionconstraintlist(c *ExclusionconstraintlistContext)

	// EnterExclusionconstraintelem is called when entering the exclusionconstraintelem production.
	EnterExclusionconstraintelem(c *ExclusionconstraintelemContext)

	// EnterExclusionwhereclause is called when entering the exclusionwhereclause production.
	EnterExclusionwhereclause(c *ExclusionwhereclauseContext)

	// EnterKey_actions is called when entering the key_actions production.
	EnterKey_actions(c *Key_actionsContext)

	// EnterKey_update is called when entering the key_update production.
	EnterKey_update(c *Key_updateContext)

	// EnterKey_delete is called when entering the key_delete production.
	EnterKey_delete(c *Key_deleteContext)

	// EnterKey_action is called when entering the key_action production.
	EnterKey_action(c *Key_actionContext)

	// EnterOptinherit is called when entering the optinherit production.
	EnterOptinherit(c *OptinheritContext)

	// EnterOptpartitionspec is called when entering the optpartitionspec production.
	EnterOptpartitionspec(c *OptpartitionspecContext)

	// EnterPartitionspec is called when entering the partitionspec production.
	EnterPartitionspec(c *PartitionspecContext)

	// EnterPart_params is called when entering the part_params production.
	EnterPart_params(c *Part_paramsContext)

	// EnterPart_elem is called when entering the part_elem production.
	EnterPart_elem(c *Part_elemContext)

	// EnterTable_access_method_clause is called when entering the table_access_method_clause production.
	EnterTable_access_method_clause(c *Table_access_method_clauseContext)

	// EnterOptwith is called when entering the optwith production.
	EnterOptwith(c *OptwithContext)

	// EnterOncommitoption is called when entering the oncommitoption production.
	EnterOncommitoption(c *OncommitoptionContext)

	// EnterOpttablespace is called when entering the opttablespace production.
	EnterOpttablespace(c *OpttablespaceContext)

	// EnterOptconstablespace is called when entering the optconstablespace production.
	EnterOptconstablespace(c *OptconstablespaceContext)

	// EnterExistingindex is called when entering the existingindex production.
	EnterExistingindex(c *ExistingindexContext)

	// EnterCreatestatsstmt is called when entering the createstatsstmt production.
	EnterCreatestatsstmt(c *CreatestatsstmtContext)

	// EnterAlterstatsstmt is called when entering the alterstatsstmt production.
	EnterAlterstatsstmt(c *AlterstatsstmtContext)

	// EnterCreateasstmt is called when entering the createasstmt production.
	EnterCreateasstmt(c *CreateasstmtContext)

	// EnterCreate_as_target is called when entering the create_as_target production.
	EnterCreate_as_target(c *Create_as_targetContext)

	// EnterWith_data_ is called when entering the with_data_ production.
	EnterWith_data_(c *With_data_Context)

	// EnterCreatematviewstmt is called when entering the creatematviewstmt production.
	EnterCreatematviewstmt(c *CreatematviewstmtContext)

	// EnterCreate_mv_target is called when entering the create_mv_target production.
	EnterCreate_mv_target(c *Create_mv_targetContext)

	// EnterOptnolog is called when entering the optnolog production.
	EnterOptnolog(c *OptnologContext)

	// EnterRefreshmatviewstmt is called when entering the refreshmatviewstmt production.
	EnterRefreshmatviewstmt(c *RefreshmatviewstmtContext)

	// EnterCreateseqstmt is called when entering the createseqstmt production.
	EnterCreateseqstmt(c *CreateseqstmtContext)

	// EnterAlterseqstmt is called when entering the alterseqstmt production.
	EnterAlterseqstmt(c *AlterseqstmtContext)

	// EnterOptseqoptlist is called when entering the optseqoptlist production.
	EnterOptseqoptlist(c *OptseqoptlistContext)

	// EnterOptparenthesizedseqoptlist is called when entering the optparenthesizedseqoptlist production.
	EnterOptparenthesizedseqoptlist(c *OptparenthesizedseqoptlistContext)

	// EnterSeqoptlist is called when entering the seqoptlist production.
	EnterSeqoptlist(c *SeqoptlistContext)

	// EnterSeqoptelem is called when entering the seqoptelem production.
	EnterSeqoptelem(c *SeqoptelemContext)

	// EnterBy_ is called when entering the by_ production.
	EnterBy_(c *By_Context)

	// EnterNumericonly is called when entering the numericonly production.
	EnterNumericonly(c *NumericonlyContext)

	// EnterNumericonly_list is called when entering the numericonly_list production.
	EnterNumericonly_list(c *Numericonly_listContext)

	// EnterCreateplangstmt is called when entering the createplangstmt production.
	EnterCreateplangstmt(c *CreateplangstmtContext)

	// EnterTrusted_ is called when entering the trusted_ production.
	EnterTrusted_(c *Trusted_Context)

	// EnterHandler_name is called when entering the handler_name production.
	EnterHandler_name(c *Handler_nameContext)

	// EnterInline_handler_ is called when entering the inline_handler_ production.
	EnterInline_handler_(c *Inline_handler_Context)

	// EnterValidator_clause is called when entering the validator_clause production.
	EnterValidator_clause(c *Validator_clauseContext)

	// EnterValidator_ is called when entering the validator_ production.
	EnterValidator_(c *Validator_Context)

	// EnterProcedural_ is called when entering the procedural_ production.
	EnterProcedural_(c *Procedural_Context)

	// EnterCreatetablespacestmt is called when entering the createtablespacestmt production.
	EnterCreatetablespacestmt(c *CreatetablespacestmtContext)

	// EnterOpttablespaceowner is called when entering the opttablespaceowner production.
	EnterOpttablespaceowner(c *OpttablespaceownerContext)

	// EnterDroptablespacestmt is called when entering the droptablespacestmt production.
	EnterDroptablespacestmt(c *DroptablespacestmtContext)

	// EnterCreateextensionstmt is called when entering the createextensionstmt production.
	EnterCreateextensionstmt(c *CreateextensionstmtContext)

	// EnterCreate_extension_opt_list is called when entering the create_extension_opt_list production.
	EnterCreate_extension_opt_list(c *Create_extension_opt_listContext)

	// EnterCreate_extension_opt_item is called when entering the create_extension_opt_item production.
	EnterCreate_extension_opt_item(c *Create_extension_opt_itemContext)

	// EnterAlterextensionstmt is called when entering the alterextensionstmt production.
	EnterAlterextensionstmt(c *AlterextensionstmtContext)

	// EnterAlter_extension_opt_list is called when entering the alter_extension_opt_list production.
	EnterAlter_extension_opt_list(c *Alter_extension_opt_listContext)

	// EnterAlter_extension_opt_item is called when entering the alter_extension_opt_item production.
	EnterAlter_extension_opt_item(c *Alter_extension_opt_itemContext)

	// EnterAlterextensioncontentsstmt is called when entering the alterextensioncontentsstmt production.
	EnterAlterextensioncontentsstmt(c *AlterextensioncontentsstmtContext)

	// EnterCreatefdwstmt is called when entering the createfdwstmt production.
	EnterCreatefdwstmt(c *CreatefdwstmtContext)

	// EnterFdw_option is called when entering the fdw_option production.
	EnterFdw_option(c *Fdw_optionContext)

	// EnterFdw_options is called when entering the fdw_options production.
	EnterFdw_options(c *Fdw_optionsContext)

	// EnterFdw_options_ is called when entering the fdw_options_ production.
	EnterFdw_options_(c *Fdw_options_Context)

	// EnterAlterfdwstmt is called when entering the alterfdwstmt production.
	EnterAlterfdwstmt(c *AlterfdwstmtContext)

	// EnterCreate_generic_options is called when entering the create_generic_options production.
	EnterCreate_generic_options(c *Create_generic_optionsContext)

	// EnterGeneric_option_list is called when entering the generic_option_list production.
	EnterGeneric_option_list(c *Generic_option_listContext)

	// EnterAlter_generic_options is called when entering the alter_generic_options production.
	EnterAlter_generic_options(c *Alter_generic_optionsContext)

	// EnterAlter_generic_option_list is called when entering the alter_generic_option_list production.
	EnterAlter_generic_option_list(c *Alter_generic_option_listContext)

	// EnterAlter_generic_option_elem is called when entering the alter_generic_option_elem production.
	EnterAlter_generic_option_elem(c *Alter_generic_option_elemContext)

	// EnterGeneric_option_elem is called when entering the generic_option_elem production.
	EnterGeneric_option_elem(c *Generic_option_elemContext)

	// EnterGeneric_option_name is called when entering the generic_option_name production.
	EnterGeneric_option_name(c *Generic_option_nameContext)

	// EnterGeneric_option_arg is called when entering the generic_option_arg production.
	EnterGeneric_option_arg(c *Generic_option_argContext)

	// EnterCreateforeignserverstmt is called when entering the createforeignserverstmt production.
	EnterCreateforeignserverstmt(c *CreateforeignserverstmtContext)

	// EnterType_ is called when entering the type_ production.
	EnterType_(c *Type_Context)

	// EnterForeign_server_version is called when entering the foreign_server_version production.
	EnterForeign_server_version(c *Foreign_server_versionContext)

	// EnterForeign_server_version_ is called when entering the foreign_server_version_ production.
	EnterForeign_server_version_(c *Foreign_server_version_Context)

	// EnterAlterforeignserverstmt is called when entering the alterforeignserverstmt production.
	EnterAlterforeignserverstmt(c *AlterforeignserverstmtContext)

	// EnterCreateforeigntablestmt is called when entering the createforeigntablestmt production.
	EnterCreateforeigntablestmt(c *CreateforeigntablestmtContext)

	// EnterImportforeignschemastmt is called when entering the importforeignschemastmt production.
	EnterImportforeignschemastmt(c *ImportforeignschemastmtContext)

	// EnterImport_qualification_type is called when entering the import_qualification_type production.
	EnterImport_qualification_type(c *Import_qualification_typeContext)

	// EnterImport_qualification is called when entering the import_qualification production.
	EnterImport_qualification(c *Import_qualificationContext)

	// EnterCreateusermappingstmt is called when entering the createusermappingstmt production.
	EnterCreateusermappingstmt(c *CreateusermappingstmtContext)

	// EnterAuth_ident is called when entering the auth_ident production.
	EnterAuth_ident(c *Auth_identContext)

	// EnterDropusermappingstmt is called when entering the dropusermappingstmt production.
	EnterDropusermappingstmt(c *DropusermappingstmtContext)

	// EnterAlterusermappingstmt is called when entering the alterusermappingstmt production.
	EnterAlterusermappingstmt(c *AlterusermappingstmtContext)

	// EnterCreatepolicystmt is called when entering the createpolicystmt production.
	EnterCreatepolicystmt(c *CreatepolicystmtContext)

	// EnterAlterpolicystmt is called when entering the alterpolicystmt production.
	EnterAlterpolicystmt(c *AlterpolicystmtContext)

	// EnterRowsecurityoptionalexpr is called when entering the rowsecurityoptionalexpr production.
	EnterRowsecurityoptionalexpr(c *RowsecurityoptionalexprContext)

	// EnterRowsecurityoptionalwithcheck is called when entering the rowsecurityoptionalwithcheck production.
	EnterRowsecurityoptionalwithcheck(c *RowsecurityoptionalwithcheckContext)

	// EnterRowsecuritydefaulttorole is called when entering the rowsecuritydefaulttorole production.
	EnterRowsecuritydefaulttorole(c *RowsecuritydefaulttoroleContext)

	// EnterRowsecurityoptionaltorole is called when entering the rowsecurityoptionaltorole production.
	EnterRowsecurityoptionaltorole(c *RowsecurityoptionaltoroleContext)

	// EnterRowsecuritydefaultpermissive is called when entering the rowsecuritydefaultpermissive production.
	EnterRowsecuritydefaultpermissive(c *RowsecuritydefaultpermissiveContext)

	// EnterRowsecuritydefaultforcmd is called when entering the rowsecuritydefaultforcmd production.
	EnterRowsecuritydefaultforcmd(c *RowsecuritydefaultforcmdContext)

	// EnterRow_security_cmd is called when entering the row_security_cmd production.
	EnterRow_security_cmd(c *Row_security_cmdContext)

	// EnterCreateamstmt is called when entering the createamstmt production.
	EnterCreateamstmt(c *CreateamstmtContext)

	// EnterAm_type is called when entering the am_type production.
	EnterAm_type(c *Am_typeContext)

	// EnterCreatetrigstmt is called when entering the createtrigstmt production.
	EnterCreatetrigstmt(c *CreatetrigstmtContext)

	// EnterTriggeractiontime is called when entering the triggeractiontime production.
	EnterTriggeractiontime(c *TriggeractiontimeContext)

	// EnterTriggerevents is called when entering the triggerevents production.
	EnterTriggerevents(c *TriggereventsContext)

	// EnterTriggeroneevent is called when entering the triggeroneevent production.
	EnterTriggeroneevent(c *TriggeroneeventContext)

	// EnterTriggerreferencing is called when entering the triggerreferencing production.
	EnterTriggerreferencing(c *TriggerreferencingContext)

	// EnterTriggertransitions is called when entering the triggertransitions production.
	EnterTriggertransitions(c *TriggertransitionsContext)

	// EnterTriggertransition is called when entering the triggertransition production.
	EnterTriggertransition(c *TriggertransitionContext)

	// EnterTransitionoldornew is called when entering the transitionoldornew production.
	EnterTransitionoldornew(c *TransitionoldornewContext)

	// EnterTransitionrowortable is called when entering the transitionrowortable production.
	EnterTransitionrowortable(c *TransitionrowortableContext)

	// EnterTransitionrelname is called when entering the transitionrelname production.
	EnterTransitionrelname(c *TransitionrelnameContext)

	// EnterTriggerforspec is called when entering the triggerforspec production.
	EnterTriggerforspec(c *TriggerforspecContext)

	// EnterTriggerforopteach is called when entering the triggerforopteach production.
	EnterTriggerforopteach(c *TriggerforopteachContext)

	// EnterTriggerfortype is called when entering the triggerfortype production.
	EnterTriggerfortype(c *TriggerfortypeContext)

	// EnterTriggerwhen is called when entering the triggerwhen production.
	EnterTriggerwhen(c *TriggerwhenContext)

	// EnterFunction_or_procedure is called when entering the function_or_procedure production.
	EnterFunction_or_procedure(c *Function_or_procedureContext)

	// EnterTriggerfuncargs is called when entering the triggerfuncargs production.
	EnterTriggerfuncargs(c *TriggerfuncargsContext)

	// EnterTriggerfuncarg is called when entering the triggerfuncarg production.
	EnterTriggerfuncarg(c *TriggerfuncargContext)

	// EnterOptconstrfromtable is called when entering the optconstrfromtable production.
	EnterOptconstrfromtable(c *OptconstrfromtableContext)

	// EnterConstraintattributespec is called when entering the constraintattributespec production.
	EnterConstraintattributespec(c *ConstraintattributespecContext)

	// EnterConstraintattributeElem is called when entering the constraintattributeElem production.
	EnterConstraintattributeElem(c *ConstraintattributeElemContext)

	// EnterCreateeventtrigstmt is called when entering the createeventtrigstmt production.
	EnterCreateeventtrigstmt(c *CreateeventtrigstmtContext)

	// EnterEvent_trigger_when_list is called when entering the event_trigger_when_list production.
	EnterEvent_trigger_when_list(c *Event_trigger_when_listContext)

	// EnterEvent_trigger_when_item is called when entering the event_trigger_when_item production.
	EnterEvent_trigger_when_item(c *Event_trigger_when_itemContext)

	// EnterEvent_trigger_value_list is called when entering the event_trigger_value_list production.
	EnterEvent_trigger_value_list(c *Event_trigger_value_listContext)

	// EnterAltereventtrigstmt is called when entering the altereventtrigstmt production.
	EnterAltereventtrigstmt(c *AltereventtrigstmtContext)

	// EnterEnable_trigger is called when entering the enable_trigger production.
	EnterEnable_trigger(c *Enable_triggerContext)

	// EnterCreateassertionstmt is called when entering the createassertionstmt production.
	EnterCreateassertionstmt(c *CreateassertionstmtContext)

	// EnterDefinestmt is called when entering the definestmt production.
	EnterDefinestmt(c *DefinestmtContext)

	// EnterDefinition is called when entering the definition production.
	EnterDefinition(c *DefinitionContext)

	// EnterDef_list is called when entering the def_list production.
	EnterDef_list(c *Def_listContext)

	// EnterDef_elem is called when entering the def_elem production.
	EnterDef_elem(c *Def_elemContext)

	// EnterDef_arg is called when entering the def_arg production.
	EnterDef_arg(c *Def_argContext)

	// EnterOld_aggr_definition is called when entering the old_aggr_definition production.
	EnterOld_aggr_definition(c *Old_aggr_definitionContext)

	// EnterOld_aggr_list is called when entering the old_aggr_list production.
	EnterOld_aggr_list(c *Old_aggr_listContext)

	// EnterOld_aggr_elem is called when entering the old_aggr_elem production.
	EnterOld_aggr_elem(c *Old_aggr_elemContext)

	// EnterEnum_val_list_ is called when entering the enum_val_list_ production.
	EnterEnum_val_list_(c *Enum_val_list_Context)

	// EnterEnum_val_list is called when entering the enum_val_list production.
	EnterEnum_val_list(c *Enum_val_listContext)

	// EnterAlterenumstmt is called when entering the alterenumstmt production.
	EnterAlterenumstmt(c *AlterenumstmtContext)

	// EnterIf_not_exists_ is called when entering the if_not_exists_ production.
	EnterIf_not_exists_(c *If_not_exists_Context)

	// EnterCreateopclassstmt is called when entering the createopclassstmt production.
	EnterCreateopclassstmt(c *CreateopclassstmtContext)

	// EnterOpclass_item_list is called when entering the opclass_item_list production.
	EnterOpclass_item_list(c *Opclass_item_listContext)

	// EnterOpclass_item is called when entering the opclass_item production.
	EnterOpclass_item(c *Opclass_itemContext)

	// EnterDefault_ is called when entering the default_ production.
	EnterDefault_(c *Default_Context)

	// EnterOpfamily_ is called when entering the opfamily_ production.
	EnterOpfamily_(c *Opfamily_Context)

	// EnterOpclass_purpose is called when entering the opclass_purpose production.
	EnterOpclass_purpose(c *Opclass_purposeContext)

	// EnterRecheck_ is called when entering the recheck_ production.
	EnterRecheck_(c *Recheck_Context)

	// EnterCreateopfamilystmt is called when entering the createopfamilystmt production.
	EnterCreateopfamilystmt(c *CreateopfamilystmtContext)

	// EnterAlteropfamilystmt is called when entering the alteropfamilystmt production.
	EnterAlteropfamilystmt(c *AlteropfamilystmtContext)

	// EnterOpclass_drop_list is called when entering the opclass_drop_list production.
	EnterOpclass_drop_list(c *Opclass_drop_listContext)

	// EnterOpclass_drop is called when entering the opclass_drop production.
	EnterOpclass_drop(c *Opclass_dropContext)

	// EnterDropopclassstmt is called when entering the dropopclassstmt production.
	EnterDropopclassstmt(c *DropopclassstmtContext)

	// EnterDropopfamilystmt is called when entering the dropopfamilystmt production.
	EnterDropopfamilystmt(c *DropopfamilystmtContext)

	// EnterDropownedstmt is called when entering the dropownedstmt production.
	EnterDropownedstmt(c *DropownedstmtContext)

	// EnterReassignownedstmt is called when entering the reassignownedstmt production.
	EnterReassignownedstmt(c *ReassignownedstmtContext)

	// EnterDropstmt is called when entering the dropstmt production.
	EnterDropstmt(c *DropstmtContext)

	// EnterObject_type_any_name is called when entering the object_type_any_name production.
	EnterObject_type_any_name(c *Object_type_any_nameContext)

	// EnterObject_type_name is called when entering the object_type_name production.
	EnterObject_type_name(c *Object_type_nameContext)

	// EnterDrop_type_name is called when entering the drop_type_name production.
	EnterDrop_type_name(c *Drop_type_nameContext)

	// EnterObject_type_name_on_any_name is called when entering the object_type_name_on_any_name production.
	EnterObject_type_name_on_any_name(c *Object_type_name_on_any_nameContext)

	// EnterAny_name_list_ is called when entering the any_name_list_ production.
	EnterAny_name_list_(c *Any_name_list_Context)

	// EnterAny_name is called when entering the any_name production.
	EnterAny_name(c *Any_nameContext)

	// EnterAttrs is called when entering the attrs production.
	EnterAttrs(c *AttrsContext)

	// EnterType_name_list is called when entering the type_name_list production.
	EnterType_name_list(c *Type_name_listContext)

	// EnterTruncatestmt is called when entering the truncatestmt production.
	EnterTruncatestmt(c *TruncatestmtContext)

	// EnterRestart_seqs_ is called when entering the restart_seqs_ production.
	EnterRestart_seqs_(c *Restart_seqs_Context)

	// EnterCommentstmt is called when entering the commentstmt production.
	EnterCommentstmt(c *CommentstmtContext)

	// EnterComment_text is called when entering the comment_text production.
	EnterComment_text(c *Comment_textContext)

	// EnterSeclabelstmt is called when entering the seclabelstmt production.
	EnterSeclabelstmt(c *SeclabelstmtContext)

	// EnterProvider_ is called when entering the provider_ production.
	EnterProvider_(c *Provider_Context)

	// EnterSecurity_label is called when entering the security_label production.
	EnterSecurity_label(c *Security_labelContext)

	// EnterFetchstmt is called when entering the fetchstmt production.
	EnterFetchstmt(c *FetchstmtContext)

	// EnterFetch_args is called when entering the fetch_args production.
	EnterFetch_args(c *Fetch_argsContext)

	// EnterFrom_in is called when entering the from_in production.
	EnterFrom_in(c *From_inContext)

	// EnterFrom_in_ is called when entering the from_in_ production.
	EnterFrom_in_(c *From_in_Context)

	// EnterGrantstmt is called when entering the grantstmt production.
	EnterGrantstmt(c *GrantstmtContext)

	// EnterRevokestmt is called when entering the revokestmt production.
	EnterRevokestmt(c *RevokestmtContext)

	// EnterPrivileges is called when entering the privileges production.
	EnterPrivileges(c *PrivilegesContext)

	// EnterPrivilege_list is called when entering the privilege_list production.
	EnterPrivilege_list(c *Privilege_listContext)

	// EnterPrivilege is called when entering the privilege production.
	EnterPrivilege(c *PrivilegeContext)

	// EnterPrivilege_target is called when entering the privilege_target production.
	EnterPrivilege_target(c *Privilege_targetContext)

	// EnterGrantee_list is called when entering the grantee_list production.
	EnterGrantee_list(c *Grantee_listContext)

	// EnterGrantee is called when entering the grantee production.
	EnterGrantee(c *GranteeContext)

	// EnterGrant_grant_option_ is called when entering the grant_grant_option_ production.
	EnterGrant_grant_option_(c *Grant_grant_option_Context)

	// EnterGrantrolestmt is called when entering the grantrolestmt production.
	EnterGrantrolestmt(c *GrantrolestmtContext)

	// EnterRevokerolestmt is called when entering the revokerolestmt production.
	EnterRevokerolestmt(c *RevokerolestmtContext)

	// EnterGrant_admin_option_ is called when entering the grant_admin_option_ production.
	EnterGrant_admin_option_(c *Grant_admin_option_Context)

	// EnterGranted_by_ is called when entering the granted_by_ production.
	EnterGranted_by_(c *Granted_by_Context)

	// EnterAlterdefaultprivilegesstmt is called when entering the alterdefaultprivilegesstmt production.
	EnterAlterdefaultprivilegesstmt(c *AlterdefaultprivilegesstmtContext)

	// EnterDefacloptionlist is called when entering the defacloptionlist production.
	EnterDefacloptionlist(c *DefacloptionlistContext)

	// EnterDefacloption is called when entering the defacloption production.
	EnterDefacloption(c *DefacloptionContext)

	// EnterDefaclaction is called when entering the defaclaction production.
	EnterDefaclaction(c *DefaclactionContext)

	// EnterDefacl_privilege_target is called when entering the defacl_privilege_target production.
	EnterDefacl_privilege_target(c *Defacl_privilege_targetContext)

	// EnterIndexstmt is called when entering the indexstmt production.
	EnterIndexstmt(c *IndexstmtContext)

	// EnterUnique_ is called when entering the unique_ production.
	EnterUnique_(c *Unique_Context)

	// EnterSingle_name_ is called when entering the single_name_ production.
	EnterSingle_name_(c *Single_name_Context)

	// EnterConcurrently_ is called when entering the concurrently_ production.
	EnterConcurrently_(c *Concurrently_Context)

	// EnterIndex_name_ is called when entering the index_name_ production.
	EnterIndex_name_(c *Index_name_Context)

	// EnterAccess_method_clause is called when entering the access_method_clause production.
	EnterAccess_method_clause(c *Access_method_clauseContext)

	// EnterIndex_params is called when entering the index_params production.
	EnterIndex_params(c *Index_paramsContext)

	// EnterIndex_elem_options is called when entering the index_elem_options production.
	EnterIndex_elem_options(c *Index_elem_optionsContext)

	// EnterIndex_elem is called when entering the index_elem production.
	EnterIndex_elem(c *Index_elemContext)

	// EnterInclude_ is called when entering the include_ production.
	EnterInclude_(c *Include_Context)

	// EnterIndex_including_params is called when entering the index_including_params production.
	EnterIndex_including_params(c *Index_including_paramsContext)

	// EnterCollate_ is called when entering the collate_ production.
	EnterCollate_(c *Collate_Context)

	// EnterClass_ is called when entering the class_ production.
	EnterClass_(c *Class_Context)

	// EnterAsc_desc_ is called when entering the asc_desc_ production.
	EnterAsc_desc_(c *Asc_desc_Context)

	// EnterNulls_order_ is called when entering the nulls_order_ production.
	EnterNulls_order_(c *Nulls_order_Context)

	// EnterCreatefunctionstmt is called when entering the createfunctionstmt production.
	EnterCreatefunctionstmt(c *CreatefunctionstmtContext)

	// EnterOr_replace_ is called when entering the or_replace_ production.
	EnterOr_replace_(c *Or_replace_Context)

	// EnterFunc_args is called when entering the func_args production.
	EnterFunc_args(c *Func_argsContext)

	// EnterFunc_args_list is called when entering the func_args_list production.
	EnterFunc_args_list(c *Func_args_listContext)

	// EnterFunction_with_argtypes_list is called when entering the function_with_argtypes_list production.
	EnterFunction_with_argtypes_list(c *Function_with_argtypes_listContext)

	// EnterFunction_with_argtypes is called when entering the function_with_argtypes production.
	EnterFunction_with_argtypes(c *Function_with_argtypesContext)

	// EnterFunc_args_with_defaults is called when entering the func_args_with_defaults production.
	EnterFunc_args_with_defaults(c *Func_args_with_defaultsContext)

	// EnterFunc_args_with_defaults_list is called when entering the func_args_with_defaults_list production.
	EnterFunc_args_with_defaults_list(c *Func_args_with_defaults_listContext)

	// EnterFunc_arg is called when entering the func_arg production.
	EnterFunc_arg(c *Func_argContext)

	// EnterArg_class is called when entering the arg_class production.
	EnterArg_class(c *Arg_classContext)

	// EnterParam_name is called when entering the param_name production.
	EnterParam_name(c *Param_nameContext)

	// EnterFunc_return is called when entering the func_return production.
	EnterFunc_return(c *Func_returnContext)

	// EnterFunc_type is called when entering the func_type production.
	EnterFunc_type(c *Func_typeContext)

	// EnterFunc_arg_with_default is called when entering the func_arg_with_default production.
	EnterFunc_arg_with_default(c *Func_arg_with_defaultContext)

	// EnterAggr_arg is called when entering the aggr_arg production.
	EnterAggr_arg(c *Aggr_argContext)

	// EnterAggr_args is called when entering the aggr_args production.
	EnterAggr_args(c *Aggr_argsContext)

	// EnterAggr_args_list is called when entering the aggr_args_list production.
	EnterAggr_args_list(c *Aggr_args_listContext)

	// EnterAggregate_with_argtypes is called when entering the aggregate_with_argtypes production.
	EnterAggregate_with_argtypes(c *Aggregate_with_argtypesContext)

	// EnterAggregate_with_argtypes_list is called when entering the aggregate_with_argtypes_list production.
	EnterAggregate_with_argtypes_list(c *Aggregate_with_argtypes_listContext)

	// EnterCreatefunc_opt_list is called when entering the createfunc_opt_list production.
	EnterCreatefunc_opt_list(c *Createfunc_opt_listContext)

	// EnterCommon_func_opt_item is called when entering the common_func_opt_item production.
	EnterCommon_func_opt_item(c *Common_func_opt_itemContext)

	// EnterCreatefunc_opt_item is called when entering the createfunc_opt_item production.
	EnterCreatefunc_opt_item(c *Createfunc_opt_itemContext)

	// EnterFunc_as is called when entering the func_as production.
	EnterFunc_as(c *Func_asContext)

	// EnterTransform_type_list is called when entering the transform_type_list production.
	EnterTransform_type_list(c *Transform_type_listContext)

	// EnterDefinition_ is called when entering the definition_ production.
	EnterDefinition_(c *Definition_Context)

	// EnterTable_func_column is called when entering the table_func_column production.
	EnterTable_func_column(c *Table_func_columnContext)

	// EnterTable_func_column_list is called when entering the table_func_column_list production.
	EnterTable_func_column_list(c *Table_func_column_listContext)

	// EnterAlterfunctionstmt is called when entering the alterfunctionstmt production.
	EnterAlterfunctionstmt(c *AlterfunctionstmtContext)

	// EnterAlterfunc_opt_list is called when entering the alterfunc_opt_list production.
	EnterAlterfunc_opt_list(c *Alterfunc_opt_listContext)

	// EnterRestrict_ is called when entering the restrict_ production.
	EnterRestrict_(c *Restrict_Context)

	// EnterRemovefuncstmt is called when entering the removefuncstmt production.
	EnterRemovefuncstmt(c *RemovefuncstmtContext)

	// EnterRemoveaggrstmt is called when entering the removeaggrstmt production.
	EnterRemoveaggrstmt(c *RemoveaggrstmtContext)

	// EnterRemoveoperstmt is called when entering the removeoperstmt production.
	EnterRemoveoperstmt(c *RemoveoperstmtContext)

	// EnterOper_argtypes is called when entering the oper_argtypes production.
	EnterOper_argtypes(c *Oper_argtypesContext)

	// EnterAny_operator is called when entering the any_operator production.
	EnterAny_operator(c *Any_operatorContext)

	// EnterOperator_with_argtypes_list is called when entering the operator_with_argtypes_list production.
	EnterOperator_with_argtypes_list(c *Operator_with_argtypes_listContext)

	// EnterOperator_with_argtypes is called when entering the operator_with_argtypes production.
	EnterOperator_with_argtypes(c *Operator_with_argtypesContext)

	// EnterDostmt is called when entering the dostmt production.
	EnterDostmt(c *DostmtContext)

	// EnterDostmt_opt_list is called when entering the dostmt_opt_list production.
	EnterDostmt_opt_list(c *Dostmt_opt_listContext)

	// EnterDostmt_opt_item is called when entering the dostmt_opt_item production.
	EnterDostmt_opt_item(c *Dostmt_opt_itemContext)

	// EnterCreatecaststmt is called when entering the createcaststmt production.
	EnterCreatecaststmt(c *CreatecaststmtContext)

	// EnterCast_context is called when entering the cast_context production.
	EnterCast_context(c *Cast_contextContext)

	// EnterDropcaststmt is called when entering the dropcaststmt production.
	EnterDropcaststmt(c *DropcaststmtContext)

	// EnterIf_exists_ is called when entering the if_exists_ production.
	EnterIf_exists_(c *If_exists_Context)

	// EnterCreatetransformstmt is called when entering the createtransformstmt production.
	EnterCreatetransformstmt(c *CreatetransformstmtContext)

	// EnterTransform_element_list is called when entering the transform_element_list production.
	EnterTransform_element_list(c *Transform_element_listContext)

	// EnterDroptransformstmt is called when entering the droptransformstmt production.
	EnterDroptransformstmt(c *DroptransformstmtContext)

	// EnterReindexstmt is called when entering the reindexstmt production.
	EnterReindexstmt(c *ReindexstmtContext)

	// EnterReindex_target_relation is called when entering the reindex_target_relation production.
	EnterReindex_target_relation(c *Reindex_target_relationContext)

	// EnterReindex_target_all is called when entering the reindex_target_all production.
	EnterReindex_target_all(c *Reindex_target_allContext)

	// EnterReindex_option_list is called when entering the reindex_option_list production.
	EnterReindex_option_list(c *Reindex_option_listContext)

	// EnterAltertblspcstmt is called when entering the altertblspcstmt production.
	EnterAltertblspcstmt(c *AltertblspcstmtContext)

	// EnterRenamestmt is called when entering the renamestmt production.
	EnterRenamestmt(c *RenamestmtContext)

	// EnterColumn_ is called when entering the column_ production.
	EnterColumn_(c *Column_Context)

	// EnterSet_data_ is called when entering the set_data_ production.
	EnterSet_data_(c *Set_data_Context)

	// EnterAlterobjectdependsstmt is called when entering the alterobjectdependsstmt production.
	EnterAlterobjectdependsstmt(c *AlterobjectdependsstmtContext)

	// EnterNo_ is called when entering the no_ production.
	EnterNo_(c *No_Context)

	// EnterAlterobjectschemastmt is called when entering the alterobjectschemastmt production.
	EnterAlterobjectschemastmt(c *AlterobjectschemastmtContext)

	// EnterAlteroperatorstmt is called when entering the alteroperatorstmt production.
	EnterAlteroperatorstmt(c *AlteroperatorstmtContext)

	// EnterOperator_def_list is called when entering the operator_def_list production.
	EnterOperator_def_list(c *Operator_def_listContext)

	// EnterOperator_def_elem is called when entering the operator_def_elem production.
	EnterOperator_def_elem(c *Operator_def_elemContext)

	// EnterOperator_def_arg is called when entering the operator_def_arg production.
	EnterOperator_def_arg(c *Operator_def_argContext)

	// EnterAltertypestmt is called when entering the altertypestmt production.
	EnterAltertypestmt(c *AltertypestmtContext)

	// EnterAlterownerstmt is called when entering the alterownerstmt production.
	EnterAlterownerstmt(c *AlterownerstmtContext)

	// EnterCreatepublicationstmt is called when entering the createpublicationstmt production.
	EnterCreatepublicationstmt(c *CreatepublicationstmtContext)

	// EnterPublication_for_tables_ is called when entering the publication_for_tables_ production.
	EnterPublication_for_tables_(c *Publication_for_tables_Context)

	// EnterPublication_for_tables is called when entering the publication_for_tables production.
	EnterPublication_for_tables(c *Publication_for_tablesContext)

	// EnterAlterpublicationstmt is called when entering the alterpublicationstmt production.
	EnterAlterpublicationstmt(c *AlterpublicationstmtContext)

	// EnterCreatesubscriptionstmt is called when entering the createsubscriptionstmt production.
	EnterCreatesubscriptionstmt(c *CreatesubscriptionstmtContext)

	// EnterPublication_name_list is called when entering the publication_name_list production.
	EnterPublication_name_list(c *Publication_name_listContext)

	// EnterPublication_name_item is called when entering the publication_name_item production.
	EnterPublication_name_item(c *Publication_name_itemContext)

	// EnterAltersubscriptionstmt is called when entering the altersubscriptionstmt production.
	EnterAltersubscriptionstmt(c *AltersubscriptionstmtContext)

	// EnterDropsubscriptionstmt is called when entering the dropsubscriptionstmt production.
	EnterDropsubscriptionstmt(c *DropsubscriptionstmtContext)

	// EnterRulestmt is called when entering the rulestmt production.
	EnterRulestmt(c *RulestmtContext)

	// EnterRuleactionlist is called when entering the ruleactionlist production.
	EnterRuleactionlist(c *RuleactionlistContext)

	// EnterRuleactionmulti is called when entering the ruleactionmulti production.
	EnterRuleactionmulti(c *RuleactionmultiContext)

	// EnterRuleactionstmt is called when entering the ruleactionstmt production.
	EnterRuleactionstmt(c *RuleactionstmtContext)

	// EnterRuleactionstmtOrEmpty is called when entering the ruleactionstmtOrEmpty production.
	EnterRuleactionstmtOrEmpty(c *RuleactionstmtOrEmptyContext)

	// EnterEvent is called when entering the event production.
	EnterEvent(c *EventContext)

	// EnterInstead_ is called when entering the instead_ production.
	EnterInstead_(c *Instead_Context)

	// EnterNotifystmt is called when entering the notifystmt production.
	EnterNotifystmt(c *NotifystmtContext)

	// EnterNotify_payload is called when entering the notify_payload production.
	EnterNotify_payload(c *Notify_payloadContext)

	// EnterListenstmt is called when entering the listenstmt production.
	EnterListenstmt(c *ListenstmtContext)

	// EnterUnlistenstmt is called when entering the unlistenstmt production.
	EnterUnlistenstmt(c *UnlistenstmtContext)

	// EnterTransactionstmt is called when entering the transactionstmt production.
	EnterTransactionstmt(c *TransactionstmtContext)

	// EnterTransaction_ is called when entering the transaction_ production.
	EnterTransaction_(c *Transaction_Context)

	// EnterTransaction_mode_item is called when entering the transaction_mode_item production.
	EnterTransaction_mode_item(c *Transaction_mode_itemContext)

	// EnterTransaction_mode_list is called when entering the transaction_mode_list production.
	EnterTransaction_mode_list(c *Transaction_mode_listContext)

	// EnterTransaction_mode_list_or_empty is called when entering the transaction_mode_list_or_empty production.
	EnterTransaction_mode_list_or_empty(c *Transaction_mode_list_or_emptyContext)

	// EnterTransaction_chain_ is called when entering the transaction_chain_ production.
	EnterTransaction_chain_(c *Transaction_chain_Context)

	// EnterViewstmt is called when entering the viewstmt production.
	EnterViewstmt(c *ViewstmtContext)

	// EnterCheck_option_ is called when entering the check_option_ production.
	EnterCheck_option_(c *Check_option_Context)

	// EnterLoadstmt is called when entering the loadstmt production.
	EnterLoadstmt(c *LoadstmtContext)

	// EnterCreatedbstmt is called when entering the createdbstmt production.
	EnterCreatedbstmt(c *CreatedbstmtContext)

	// EnterCreatedb_opt_list is called when entering the createdb_opt_list production.
	EnterCreatedb_opt_list(c *Createdb_opt_listContext)

	// EnterCreatedb_opt_items is called when entering the createdb_opt_items production.
	EnterCreatedb_opt_items(c *Createdb_opt_itemsContext)

	// EnterCreatedb_opt_item is called when entering the createdb_opt_item production.
	EnterCreatedb_opt_item(c *Createdb_opt_itemContext)

	// EnterCreatedb_opt_name is called when entering the createdb_opt_name production.
	EnterCreatedb_opt_name(c *Createdb_opt_nameContext)

	// EnterEqual_ is called when entering the equal_ production.
	EnterEqual_(c *Equal_Context)

	// EnterAlterdatabasestmt is called when entering the alterdatabasestmt production.
	EnterAlterdatabasestmt(c *AlterdatabasestmtContext)

	// EnterAlterdatabasesetstmt is called when entering the alterdatabasesetstmt production.
	EnterAlterdatabasesetstmt(c *AlterdatabasesetstmtContext)

	// EnterDropdbstmt is called when entering the dropdbstmt production.
	EnterDropdbstmt(c *DropdbstmtContext)

	// EnterDrop_option_list is called when entering the drop_option_list production.
	EnterDrop_option_list(c *Drop_option_listContext)

	// EnterDrop_option is called when entering the drop_option production.
	EnterDrop_option(c *Drop_optionContext)

	// EnterAltercollationstmt is called when entering the altercollationstmt production.
	EnterAltercollationstmt(c *AltercollationstmtContext)

	// EnterAltersystemstmt is called when entering the altersystemstmt production.
	EnterAltersystemstmt(c *AltersystemstmtContext)

	// EnterCreatedomainstmt is called when entering the createdomainstmt production.
	EnterCreatedomainstmt(c *CreatedomainstmtContext)

	// EnterAlterdomainstmt is called when entering the alterdomainstmt production.
	EnterAlterdomainstmt(c *AlterdomainstmtContext)

	// EnterAs_ is called when entering the as_ production.
	EnterAs_(c *As_Context)

	// EnterAltertsdictionarystmt is called when entering the altertsdictionarystmt production.
	EnterAltertsdictionarystmt(c *AltertsdictionarystmtContext)

	// EnterAltertsconfigurationstmt is called when entering the altertsconfigurationstmt production.
	EnterAltertsconfigurationstmt(c *AltertsconfigurationstmtContext)

	// EnterAny_with is called when entering the any_with production.
	EnterAny_with(c *Any_withContext)

	// EnterCreateconversionstmt is called when entering the createconversionstmt production.
	EnterCreateconversionstmt(c *CreateconversionstmtContext)

	// EnterClusterstmt is called when entering the clusterstmt production.
	EnterClusterstmt(c *ClusterstmtContext)

	// EnterCluster_index_specification is called when entering the cluster_index_specification production.
	EnterCluster_index_specification(c *Cluster_index_specificationContext)

	// EnterVacuumstmt is called when entering the vacuumstmt production.
	EnterVacuumstmt(c *VacuumstmtContext)

	// EnterAnalyzestmt is called when entering the analyzestmt production.
	EnterAnalyzestmt(c *AnalyzestmtContext)

	// EnterUtility_option_list is called when entering the utility_option_list production.
	EnterUtility_option_list(c *Utility_option_listContext)

	// EnterVac_analyze_option_list is called when entering the vac_analyze_option_list production.
	EnterVac_analyze_option_list(c *Vac_analyze_option_listContext)

	// EnterAnalyze_keyword is called when entering the analyze_keyword production.
	EnterAnalyze_keyword(c *Analyze_keywordContext)

	// EnterUtility_option_elem is called when entering the utility_option_elem production.
	EnterUtility_option_elem(c *Utility_option_elemContext)

	// EnterUtility_option_name is called when entering the utility_option_name production.
	EnterUtility_option_name(c *Utility_option_nameContext)

	// EnterUtility_option_arg is called when entering the utility_option_arg production.
	EnterUtility_option_arg(c *Utility_option_argContext)

	// EnterVac_analyze_option_elem is called when entering the vac_analyze_option_elem production.
	EnterVac_analyze_option_elem(c *Vac_analyze_option_elemContext)

	// EnterVac_analyze_option_name is called when entering the vac_analyze_option_name production.
	EnterVac_analyze_option_name(c *Vac_analyze_option_nameContext)

	// EnterVac_analyze_option_arg is called when entering the vac_analyze_option_arg production.
	EnterVac_analyze_option_arg(c *Vac_analyze_option_argContext)

	// EnterAnalyze_ is called when entering the analyze_ production.
	EnterAnalyze_(c *Analyze_Context)

	// EnterVerbose_ is called when entering the verbose_ production.
	EnterVerbose_(c *Verbose_Context)

	// EnterFull_ is called when entering the full_ production.
	EnterFull_(c *Full_Context)

	// EnterFreeze_ is called when entering the freeze_ production.
	EnterFreeze_(c *Freeze_Context)

	// EnterName_list_ is called when entering the name_list_ production.
	EnterName_list_(c *Name_list_Context)

	// EnterVacuum_relation is called when entering the vacuum_relation production.
	EnterVacuum_relation(c *Vacuum_relationContext)

	// EnterVacuum_relation_list is called when entering the vacuum_relation_list production.
	EnterVacuum_relation_list(c *Vacuum_relation_listContext)

	// EnterVacuum_relation_list_ is called when entering the vacuum_relation_list_ production.
	EnterVacuum_relation_list_(c *Vacuum_relation_list_Context)

	// EnterExplainstmt is called when entering the explainstmt production.
	EnterExplainstmt(c *ExplainstmtContext)

	// EnterExplainablestmt is called when entering the explainablestmt production.
	EnterExplainablestmt(c *ExplainablestmtContext)

	// EnterExplain_option_list is called when entering the explain_option_list production.
	EnterExplain_option_list(c *Explain_option_listContext)

	// EnterExplain_option_elem is called when entering the explain_option_elem production.
	EnterExplain_option_elem(c *Explain_option_elemContext)

	// EnterExplain_option_name is called when entering the explain_option_name production.
	EnterExplain_option_name(c *Explain_option_nameContext)

	// EnterExplain_option_arg is called when entering the explain_option_arg production.
	EnterExplain_option_arg(c *Explain_option_argContext)

	// EnterPreparestmt is called when entering the preparestmt production.
	EnterPreparestmt(c *PreparestmtContext)

	// EnterPrep_type_clause is called when entering the prep_type_clause production.
	EnterPrep_type_clause(c *Prep_type_clauseContext)

	// EnterPreparablestmt is called when entering the preparablestmt production.
	EnterPreparablestmt(c *PreparablestmtContext)

	// EnterExecutestmt is called when entering the executestmt production.
	EnterExecutestmt(c *ExecutestmtContext)

	// EnterExecute_param_clause is called when entering the execute_param_clause production.
	EnterExecute_param_clause(c *Execute_param_clauseContext)

	// EnterDeallocatestmt is called when entering the deallocatestmt production.
	EnterDeallocatestmt(c *DeallocatestmtContext)

	// EnterInsertstmt is called when entering the insertstmt production.
	EnterInsertstmt(c *InsertstmtContext)

	// EnterInsert_target is called when entering the insert_target production.
	EnterInsert_target(c *Insert_targetContext)

	// EnterInsert_rest is called when entering the insert_rest production.
	EnterInsert_rest(c *Insert_restContext)

	// EnterOverride_kind is called when entering the override_kind production.
	EnterOverride_kind(c *Override_kindContext)

	// EnterInsert_column_list is called when entering the insert_column_list production.
	EnterInsert_column_list(c *Insert_column_listContext)

	// EnterInsert_column_item is called when entering the insert_column_item production.
	EnterInsert_column_item(c *Insert_column_itemContext)

	// EnterOn_conflict_ is called when entering the on_conflict_ production.
	EnterOn_conflict_(c *On_conflict_Context)

	// EnterConf_expr_ is called when entering the conf_expr_ production.
	EnterConf_expr_(c *Conf_expr_Context)

	// EnterReturning_clause is called when entering the returning_clause production.
	EnterReturning_clause(c *Returning_clauseContext)

	// EnterMergestmt is called when entering the mergestmt production.
	EnterMergestmt(c *MergestmtContext)

	// EnterMerge_insert_clause is called when entering the merge_insert_clause production.
	EnterMerge_insert_clause(c *Merge_insert_clauseContext)

	// EnterMerge_update_clause is called when entering the merge_update_clause production.
	EnterMerge_update_clause(c *Merge_update_clauseContext)

	// EnterMerge_delete_clause is called when entering the merge_delete_clause production.
	EnterMerge_delete_clause(c *Merge_delete_clauseContext)

	// EnterDeletestmt is called when entering the deletestmt production.
	EnterDeletestmt(c *DeletestmtContext)

	// EnterUsing_clause is called when entering the using_clause production.
	EnterUsing_clause(c *Using_clauseContext)

	// EnterLockstmt is called when entering the lockstmt production.
	EnterLockstmt(c *LockstmtContext)

	// EnterLock_ is called when entering the lock_ production.
	EnterLock_(c *Lock_Context)

	// EnterLock_type is called when entering the lock_type production.
	EnterLock_type(c *Lock_typeContext)

	// EnterNowait_ is called when entering the nowait_ production.
	EnterNowait_(c *Nowait_Context)

	// EnterNowait_or_skip_ is called when entering the nowait_or_skip_ production.
	EnterNowait_or_skip_(c *Nowait_or_skip_Context)

	// EnterUpdatestmt is called when entering the updatestmt production.
	EnterUpdatestmt(c *UpdatestmtContext)

	// EnterSet_clause_list is called when entering the set_clause_list production.
	EnterSet_clause_list(c *Set_clause_listContext)

	// EnterSet_clause is called when entering the set_clause production.
	EnterSet_clause(c *Set_clauseContext)

	// EnterSet_target is called when entering the set_target production.
	EnterSet_target(c *Set_targetContext)

	// EnterSet_target_list is called when entering the set_target_list production.
	EnterSet_target_list(c *Set_target_listContext)

	// EnterDeclarecursorstmt is called when entering the declarecursorstmt production.
	EnterDeclarecursorstmt(c *DeclarecursorstmtContext)

	// EnterCursor_name is called when entering the cursor_name production.
	EnterCursor_name(c *Cursor_nameContext)

	// EnterCursor_options is called when entering the cursor_options production.
	EnterCursor_options(c *Cursor_optionsContext)

	// EnterHold_ is called when entering the hold_ production.
	EnterHold_(c *Hold_Context)

	// EnterSelectstmt is called when entering the selectstmt production.
	EnterSelectstmt(c *SelectstmtContext)

	// EnterSelect_with_parens is called when entering the select_with_parens production.
	EnterSelect_with_parens(c *Select_with_parensContext)

	// EnterSelect_no_parens is called when entering the select_no_parens production.
	EnterSelect_no_parens(c *Select_no_parensContext)

	// EnterSelect_clause is called when entering the select_clause production.
	EnterSelect_clause(c *Select_clauseContext)

	// EnterSimple_select_intersect is called when entering the simple_select_intersect production.
	EnterSimple_select_intersect(c *Simple_select_intersectContext)

	// EnterSimple_select_pramary is called when entering the simple_select_pramary production.
	EnterSimple_select_pramary(c *Simple_select_pramaryContext)

	// EnterWith_clause is called when entering the with_clause production.
	EnterWith_clause(c *With_clauseContext)

	// EnterCte_list is called when entering the cte_list production.
	EnterCte_list(c *Cte_listContext)

	// EnterCommon_table_expr is called when entering the common_table_expr production.
	EnterCommon_table_expr(c *Common_table_exprContext)

	// EnterMaterialized_ is called when entering the materialized_ production.
	EnterMaterialized_(c *Materialized_Context)

	// EnterWith_clause_ is called when entering the with_clause_ production.
	EnterWith_clause_(c *With_clause_Context)

	// EnterInto_clause is called when entering the into_clause production.
	EnterInto_clause(c *Into_clauseContext)

	// EnterStrict_ is called when entering the strict_ production.
	EnterStrict_(c *Strict_Context)

	// EnterOpttempTableName is called when entering the opttempTableName production.
	EnterOpttempTableName(c *OpttempTableNameContext)

	// EnterTable_ is called when entering the table_ production.
	EnterTable_(c *Table_Context)

	// EnterAll_or_distinct is called when entering the all_or_distinct production.
	EnterAll_or_distinct(c *All_or_distinctContext)

	// EnterDistinct_clause is called when entering the distinct_clause production.
	EnterDistinct_clause(c *Distinct_clauseContext)

	// EnterAll_clause_ is called when entering the all_clause_ production.
	EnterAll_clause_(c *All_clause_Context)

	// EnterSort_clause_ is called when entering the sort_clause_ production.
	EnterSort_clause_(c *Sort_clause_Context)

	// EnterSort_clause is called when entering the sort_clause production.
	EnterSort_clause(c *Sort_clauseContext)

	// EnterSortby_list is called when entering the sortby_list production.
	EnterSortby_list(c *Sortby_listContext)

	// EnterSortby is called when entering the sortby production.
	EnterSortby(c *SortbyContext)

	// EnterSelect_limit is called when entering the select_limit production.
	EnterSelect_limit(c *Select_limitContext)

	// EnterSelect_limit_ is called when entering the select_limit_ production.
	EnterSelect_limit_(c *Select_limit_Context)

	// EnterLimit_clause is called when entering the limit_clause production.
	EnterLimit_clause(c *Limit_clauseContext)

	// EnterOffset_clause is called when entering the offset_clause production.
	EnterOffset_clause(c *Offset_clauseContext)

	// EnterSelect_limit_value is called when entering the select_limit_value production.
	EnterSelect_limit_value(c *Select_limit_valueContext)

	// EnterSelect_offset_value is called when entering the select_offset_value production.
	EnterSelect_offset_value(c *Select_offset_valueContext)

	// EnterSelect_fetch_first_value is called when entering the select_fetch_first_value production.
	EnterSelect_fetch_first_value(c *Select_fetch_first_valueContext)

	// EnterI_or_f_const is called when entering the i_or_f_const production.
	EnterI_or_f_const(c *I_or_f_constContext)

	// EnterRow_or_rows is called when entering the row_or_rows production.
	EnterRow_or_rows(c *Row_or_rowsContext)

	// EnterFirst_or_next is called when entering the first_or_next production.
	EnterFirst_or_next(c *First_or_nextContext)

	// EnterGroup_clause is called when entering the group_clause production.
	EnterGroup_clause(c *Group_clauseContext)

	// EnterGroup_by_list is called when entering the group_by_list production.
	EnterGroup_by_list(c *Group_by_listContext)

	// EnterGroup_by_item is called when entering the group_by_item production.
	EnterGroup_by_item(c *Group_by_itemContext)

	// EnterEmpty_grouping_set is called when entering the empty_grouping_set production.
	EnterEmpty_grouping_set(c *Empty_grouping_setContext)

	// EnterRollup_clause is called when entering the rollup_clause production.
	EnterRollup_clause(c *Rollup_clauseContext)

	// EnterCube_clause is called when entering the cube_clause production.
	EnterCube_clause(c *Cube_clauseContext)

	// EnterGrouping_sets_clause is called when entering the grouping_sets_clause production.
	EnterGrouping_sets_clause(c *Grouping_sets_clauseContext)

	// EnterHaving_clause is called when entering the having_clause production.
	EnterHaving_clause(c *Having_clauseContext)

	// EnterFor_locking_clause is called when entering the for_locking_clause production.
	EnterFor_locking_clause(c *For_locking_clauseContext)

	// EnterFor_locking_clause_ is called when entering the for_locking_clause_ production.
	EnterFor_locking_clause_(c *For_locking_clause_Context)

	// EnterFor_locking_items is called when entering the for_locking_items production.
	EnterFor_locking_items(c *For_locking_itemsContext)

	// EnterFor_locking_item is called when entering the for_locking_item production.
	EnterFor_locking_item(c *For_locking_itemContext)

	// EnterFor_locking_strength is called when entering the for_locking_strength production.
	EnterFor_locking_strength(c *For_locking_strengthContext)

	// EnterLocked_rels_list is called when entering the locked_rels_list production.
	EnterLocked_rels_list(c *Locked_rels_listContext)

	// EnterValues_clause is called when entering the values_clause production.
	EnterValues_clause(c *Values_clauseContext)

	// EnterFrom_clause is called when entering the from_clause production.
	EnterFrom_clause(c *From_clauseContext)

	// EnterFrom_list is called when entering the from_list production.
	EnterFrom_list(c *From_listContext)

	// EnterTable_ref is called when entering the table_ref production.
	EnterTable_ref(c *Table_refContext)

	// EnterAlias_clause is called when entering the alias_clause production.
	EnterAlias_clause(c *Alias_clauseContext)

	// EnterFunc_alias_clause is called when entering the func_alias_clause production.
	EnterFunc_alias_clause(c *Func_alias_clauseContext)

	// EnterJoin_type is called when entering the join_type production.
	EnterJoin_type(c *Join_typeContext)

	// EnterJoin_qual is called when entering the join_qual production.
	EnterJoin_qual(c *Join_qualContext)

	// EnterRelation_expr is called when entering the relation_expr production.
	EnterRelation_expr(c *Relation_exprContext)

	// EnterRelation_expr_list is called when entering the relation_expr_list production.
	EnterRelation_expr_list(c *Relation_expr_listContext)

	// EnterRelation_expr_opt_alias is called when entering the relation_expr_opt_alias production.
	EnterRelation_expr_opt_alias(c *Relation_expr_opt_aliasContext)

	// EnterTablesample_clause is called when entering the tablesample_clause production.
	EnterTablesample_clause(c *Tablesample_clauseContext)

	// EnterRepeatable_clause_ is called when entering the repeatable_clause_ production.
	EnterRepeatable_clause_(c *Repeatable_clause_Context)

	// EnterFunc_table is called when entering the func_table production.
	EnterFunc_table(c *Func_tableContext)

	// EnterRowsfrom_item is called when entering the rowsfrom_item production.
	EnterRowsfrom_item(c *Rowsfrom_itemContext)

	// EnterRowsfrom_list is called when entering the rowsfrom_list production.
	EnterRowsfrom_list(c *Rowsfrom_listContext)

	// EnterCol_def_list_ is called when entering the col_def_list_ production.
	EnterCol_def_list_(c *Col_def_list_Context)

	// EnterOrdinality_ is called when entering the ordinality_ production.
	EnterOrdinality_(c *Ordinality_Context)

	// EnterWhere_clause is called when entering the where_clause production.
	EnterWhere_clause(c *Where_clauseContext)

	// EnterWhere_or_current_clause is called when entering the where_or_current_clause production.
	EnterWhere_or_current_clause(c *Where_or_current_clauseContext)

	// EnterOpttablefuncelementlist is called when entering the opttablefuncelementlist production.
	EnterOpttablefuncelementlist(c *OpttablefuncelementlistContext)

	// EnterTablefuncelementlist is called when entering the tablefuncelementlist production.
	EnterTablefuncelementlist(c *TablefuncelementlistContext)

	// EnterTablefuncelement is called when entering the tablefuncelement production.
	EnterTablefuncelement(c *TablefuncelementContext)

	// EnterXmltable is called when entering the xmltable production.
	EnterXmltable(c *XmltableContext)

	// EnterXmltable_column_list is called when entering the xmltable_column_list production.
	EnterXmltable_column_list(c *Xmltable_column_listContext)

	// EnterXmltable_column_el is called when entering the xmltable_column_el production.
	EnterXmltable_column_el(c *Xmltable_column_elContext)

	// EnterXmltable_column_option_list is called when entering the xmltable_column_option_list production.
	EnterXmltable_column_option_list(c *Xmltable_column_option_listContext)

	// EnterXmltable_column_option_el is called when entering the xmltable_column_option_el production.
	EnterXmltable_column_option_el(c *Xmltable_column_option_elContext)

	// EnterXml_namespace_list is called when entering the xml_namespace_list production.
	EnterXml_namespace_list(c *Xml_namespace_listContext)

	// EnterXml_namespace_el is called when entering the xml_namespace_el production.
	EnterXml_namespace_el(c *Xml_namespace_elContext)

	// EnterTypename is called when entering the typename production.
	EnterTypename(c *TypenameContext)

	// EnterOpt_array_bounds is called when entering the opt_array_bounds production.
	EnterOpt_array_bounds(c *Opt_array_boundsContext)

	// EnterSimpletypename is called when entering the simpletypename production.
	EnterSimpletypename(c *SimpletypenameContext)

	// EnterConsttypename is called when entering the consttypename production.
	EnterConsttypename(c *ConsttypenameContext)

	// EnterGenerictype is called when entering the generictype production.
	EnterGenerictype(c *GenerictypeContext)

	// EnterType_modifiers_ is called when entering the type_modifiers_ production.
	EnterType_modifiers_(c *Type_modifiers_Context)

	// EnterNumeric is called when entering the numeric production.
	EnterNumeric(c *NumericContext)

	// EnterFloat_ is called when entering the float_ production.
	EnterFloat_(c *Float_Context)

	// EnterBit is called when entering the bit production.
	EnterBit(c *BitContext)

	// EnterConstbit is called when entering the constbit production.
	EnterConstbit(c *ConstbitContext)

	// EnterBitwithlength is called when entering the bitwithlength production.
	EnterBitwithlength(c *BitwithlengthContext)

	// EnterBitwithoutlength is called when entering the bitwithoutlength production.
	EnterBitwithoutlength(c *BitwithoutlengthContext)

	// EnterCharacter is called when entering the character production.
	EnterCharacter(c *CharacterContext)

	// EnterConstcharacter is called when entering the constcharacter production.
	EnterConstcharacter(c *ConstcharacterContext)

	// EnterCharacter_c is called when entering the character_c production.
	EnterCharacter_c(c *Character_cContext)

	// EnterVarying_ is called when entering the varying_ production.
	EnterVarying_(c *Varying_Context)

	// EnterConstdatetime is called when entering the constdatetime production.
	EnterConstdatetime(c *ConstdatetimeContext)

	// EnterConstinterval is called when entering the constinterval production.
	EnterConstinterval(c *ConstintervalContext)

	// EnterTimezone_ is called when entering the timezone_ production.
	EnterTimezone_(c *Timezone_Context)

	// EnterInterval_ is called when entering the interval_ production.
	EnterInterval_(c *Interval_Context)

	// EnterInterval_second is called when entering the interval_second production.
	EnterInterval_second(c *Interval_secondContext)

	// EnterJsonType is called when entering the jsonType production.
	EnterJsonType(c *JsonTypeContext)

	// EnterEscape_ is called when entering the escape_ production.
	EnterEscape_(c *Escape_Context)

	// EnterA_expr is called when entering the a_expr production.
	EnterA_expr(c *A_exprContext)

	// EnterA_expr_qual is called when entering the a_expr_qual production.
	EnterA_expr_qual(c *A_expr_qualContext)

	// EnterA_expr_lessless is called when entering the a_expr_lessless production.
	EnterA_expr_lessless(c *A_expr_lesslessContext)

	// EnterA_expr_or is called when entering the a_expr_or production.
	EnterA_expr_or(c *A_expr_orContext)

	// EnterA_expr_and is called when entering the a_expr_and production.
	EnterA_expr_and(c *A_expr_andContext)

	// EnterA_expr_between is called when entering the a_expr_between production.
	EnterA_expr_between(c *A_expr_betweenContext)

	// EnterA_expr_in is called when entering the a_expr_in production.
	EnterA_expr_in(c *A_expr_inContext)

	// EnterA_expr_unary_not is called when entering the a_expr_unary_not production.
	EnterA_expr_unary_not(c *A_expr_unary_notContext)

	// EnterA_expr_isnull is called when entering the a_expr_isnull production.
	EnterA_expr_isnull(c *A_expr_isnullContext)

	// EnterA_expr_is_not is called when entering the a_expr_is_not production.
	EnterA_expr_is_not(c *A_expr_is_notContext)

	// EnterA_expr_compare is called when entering the a_expr_compare production.
	EnterA_expr_compare(c *A_expr_compareContext)

	// EnterA_expr_like is called when entering the a_expr_like production.
	EnterA_expr_like(c *A_expr_likeContext)

	// EnterA_expr_qual_op is called when entering the a_expr_qual_op production.
	EnterA_expr_qual_op(c *A_expr_qual_opContext)

	// EnterA_expr_unary_qualop is called when entering the a_expr_unary_qualop production.
	EnterA_expr_unary_qualop(c *A_expr_unary_qualopContext)

	// EnterA_expr_add is called when entering the a_expr_add production.
	EnterA_expr_add(c *A_expr_addContext)

	// EnterA_expr_mul is called when entering the a_expr_mul production.
	EnterA_expr_mul(c *A_expr_mulContext)

	// EnterA_expr_caret is called when entering the a_expr_caret production.
	EnterA_expr_caret(c *A_expr_caretContext)

	// EnterA_expr_unary_sign is called when entering the a_expr_unary_sign production.
	EnterA_expr_unary_sign(c *A_expr_unary_signContext)

	// EnterA_expr_at_time_zone is called when entering the a_expr_at_time_zone production.
	EnterA_expr_at_time_zone(c *A_expr_at_time_zoneContext)

	// EnterA_expr_collate is called when entering the a_expr_collate production.
	EnterA_expr_collate(c *A_expr_collateContext)

	// EnterA_expr_typecast is called when entering the a_expr_typecast production.
	EnterA_expr_typecast(c *A_expr_typecastContext)

	// EnterB_expr is called when entering the b_expr production.
	EnterB_expr(c *B_exprContext)

	// EnterC_expr_exists is called when entering the c_expr_exists production.
	EnterC_expr_exists(c *C_expr_existsContext)

	// EnterC_expr_expr is called when entering the c_expr_expr production.
	EnterC_expr_expr(c *C_expr_exprContext)

	// EnterC_expr_case is called when entering the c_expr_case production.
	EnterC_expr_case(c *C_expr_caseContext)

	// EnterPlsqlvariablename is called when entering the plsqlvariablename production.
	EnterPlsqlvariablename(c *PlsqlvariablenameContext)

	// EnterFunc_application is called when entering the func_application production.
	EnterFunc_application(c *Func_applicationContext)

	// EnterFunc_expr is called when entering the func_expr production.
	EnterFunc_expr(c *Func_exprContext)

	// EnterFunc_expr_windowless is called when entering the func_expr_windowless production.
	EnterFunc_expr_windowless(c *Func_expr_windowlessContext)

	// EnterFunc_expr_common_subexpr is called when entering the func_expr_common_subexpr production.
	EnterFunc_expr_common_subexpr(c *Func_expr_common_subexprContext)

	// EnterXml_root_version is called when entering the xml_root_version production.
	EnterXml_root_version(c *Xml_root_versionContext)

	// EnterXml_root_standalone_ is called when entering the xml_root_standalone_ production.
	EnterXml_root_standalone_(c *Xml_root_standalone_Context)

	// EnterXml_attributes is called when entering the xml_attributes production.
	EnterXml_attributes(c *Xml_attributesContext)

	// EnterXml_attribute_list is called when entering the xml_attribute_list production.
	EnterXml_attribute_list(c *Xml_attribute_listContext)

	// EnterXml_attribute_el is called when entering the xml_attribute_el production.
	EnterXml_attribute_el(c *Xml_attribute_elContext)

	// EnterDocument_or_content is called when entering the document_or_content production.
	EnterDocument_or_content(c *Document_or_contentContext)

	// EnterXml_whitespace_option is called when entering the xml_whitespace_option production.
	EnterXml_whitespace_option(c *Xml_whitespace_optionContext)

	// EnterXmlexists_argument is called when entering the xmlexists_argument production.
	EnterXmlexists_argument(c *Xmlexists_argumentContext)

	// EnterXml_passing_mech is called when entering the xml_passing_mech production.
	EnterXml_passing_mech(c *Xml_passing_mechContext)

	// EnterWithin_group_clause is called when entering the within_group_clause production.
	EnterWithin_group_clause(c *Within_group_clauseContext)

	// EnterFilter_clause is called when entering the filter_clause production.
	EnterFilter_clause(c *Filter_clauseContext)

	// EnterWindow_clause is called when entering the window_clause production.
	EnterWindow_clause(c *Window_clauseContext)

	// EnterWindow_definition_list is called when entering the window_definition_list production.
	EnterWindow_definition_list(c *Window_definition_listContext)

	// EnterWindow_definition is called when entering the window_definition production.
	EnterWindow_definition(c *Window_definitionContext)

	// EnterOver_clause is called when entering the over_clause production.
	EnterOver_clause(c *Over_clauseContext)

	// EnterWindow_specification is called when entering the window_specification production.
	EnterWindow_specification(c *Window_specificationContext)

	// EnterExisting_window_name_ is called when entering the existing_window_name_ production.
	EnterExisting_window_name_(c *Existing_window_name_Context)

	// EnterPartition_clause_ is called when entering the partition_clause_ production.
	EnterPartition_clause_(c *Partition_clause_Context)

	// EnterFrame_clause_ is called when entering the frame_clause_ production.
	EnterFrame_clause_(c *Frame_clause_Context)

	// EnterFrame_extent is called when entering the frame_extent production.
	EnterFrame_extent(c *Frame_extentContext)

	// EnterFrame_bound is called when entering the frame_bound production.
	EnterFrame_bound(c *Frame_boundContext)

	// EnterWindow_exclusion_clause_ is called when entering the window_exclusion_clause_ production.
	EnterWindow_exclusion_clause_(c *Window_exclusion_clause_Context)

	// EnterRow is called when entering the row production.
	EnterRow(c *RowContext)

	// EnterExplicit_row is called when entering the explicit_row production.
	EnterExplicit_row(c *Explicit_rowContext)

	// EnterImplicit_row is called when entering the implicit_row production.
	EnterImplicit_row(c *Implicit_rowContext)

	// EnterSub_type is called when entering the sub_type production.
	EnterSub_type(c *Sub_typeContext)

	// EnterAll_op is called when entering the all_op production.
	EnterAll_op(c *All_opContext)

	// EnterMathop is called when entering the mathop production.
	EnterMathop(c *MathopContext)

	// EnterQual_op is called when entering the qual_op production.
	EnterQual_op(c *Qual_opContext)

	// EnterQual_all_op is called when entering the qual_all_op production.
	EnterQual_all_op(c *Qual_all_opContext)

	// EnterSubquery_Op is called when entering the subquery_Op production.
	EnterSubquery_Op(c *Subquery_OpContext)

	// EnterExpr_list is called when entering the expr_list production.
	EnterExpr_list(c *Expr_listContext)

	// EnterFunc_arg_list is called when entering the func_arg_list production.
	EnterFunc_arg_list(c *Func_arg_listContext)

	// EnterFunc_arg_expr is called when entering the func_arg_expr production.
	EnterFunc_arg_expr(c *Func_arg_exprContext)

	// EnterType_list is called when entering the type_list production.
	EnterType_list(c *Type_listContext)

	// EnterArray_expr is called when entering the array_expr production.
	EnterArray_expr(c *Array_exprContext)

	// EnterArray_expr_list is called when entering the array_expr_list production.
	EnterArray_expr_list(c *Array_expr_listContext)

	// EnterExtract_list is called when entering the extract_list production.
	EnterExtract_list(c *Extract_listContext)

	// EnterExtract_arg is called when entering the extract_arg production.
	EnterExtract_arg(c *Extract_argContext)

	// EnterUnicode_normal_form is called when entering the unicode_normal_form production.
	EnterUnicode_normal_form(c *Unicode_normal_formContext)

	// EnterOverlay_list is called when entering the overlay_list production.
	EnterOverlay_list(c *Overlay_listContext)

	// EnterPosition_list is called when entering the position_list production.
	EnterPosition_list(c *Position_listContext)

	// EnterSubstr_list is called when entering the substr_list production.
	EnterSubstr_list(c *Substr_listContext)

	// EnterTrim_list is called when entering the trim_list production.
	EnterTrim_list(c *Trim_listContext)

	// EnterIn_expr_select is called when entering the in_expr_select production.
	EnterIn_expr_select(c *In_expr_selectContext)

	// EnterIn_expr_list is called when entering the in_expr_list production.
	EnterIn_expr_list(c *In_expr_listContext)

	// EnterCase_expr is called when entering the case_expr production.
	EnterCase_expr(c *Case_exprContext)

	// EnterWhen_clause_list is called when entering the when_clause_list production.
	EnterWhen_clause_list(c *When_clause_listContext)

	// EnterWhen_clause is called when entering the when_clause production.
	EnterWhen_clause(c *When_clauseContext)

	// EnterCase_default is called when entering the case_default production.
	EnterCase_default(c *Case_defaultContext)

	// EnterCase_arg is called when entering the case_arg production.
	EnterCase_arg(c *Case_argContext)

	// EnterColumnref is called when entering the columnref production.
	EnterColumnref(c *ColumnrefContext)

	// EnterIndirection_el is called when entering the indirection_el production.
	EnterIndirection_el(c *Indirection_elContext)

	// EnterSlice_bound_ is called when entering the slice_bound_ production.
	EnterSlice_bound_(c *Slice_bound_Context)

	// EnterIndirection is called when entering the indirection production.
	EnterIndirection(c *IndirectionContext)

	// EnterOpt_indirection is called when entering the opt_indirection production.
	EnterOpt_indirection(c *Opt_indirectionContext)

	// EnterJson_passing_clause is called when entering the json_passing_clause production.
	EnterJson_passing_clause(c *Json_passing_clauseContext)

	// EnterJson_arguments is called when entering the json_arguments production.
	EnterJson_arguments(c *Json_argumentsContext)

	// EnterJson_argument is called when entering the json_argument production.
	EnterJson_argument(c *Json_argumentContext)

	// EnterJson_wrapper_behavior is called when entering the json_wrapper_behavior production.
	EnterJson_wrapper_behavior(c *Json_wrapper_behaviorContext)

	// EnterJson_behavior is called when entering the json_behavior production.
	EnterJson_behavior(c *Json_behaviorContext)

	// EnterJson_behavior_type is called when entering the json_behavior_type production.
	EnterJson_behavior_type(c *Json_behavior_typeContext)

	// EnterJson_behavior_clause is called when entering the json_behavior_clause production.
	EnterJson_behavior_clause(c *Json_behavior_clauseContext)

	// EnterJson_on_error_clause is called when entering the json_on_error_clause production.
	EnterJson_on_error_clause(c *Json_on_error_clauseContext)

	// EnterJson_value_expr is called when entering the json_value_expr production.
	EnterJson_value_expr(c *Json_value_exprContext)

	// EnterJson_format_clause is called when entering the json_format_clause production.
	EnterJson_format_clause(c *Json_format_clauseContext)

	// EnterJson_quotes_clause is called when entering the json_quotes_clause production.
	EnterJson_quotes_clause(c *Json_quotes_clauseContext)

	// EnterJson_returning_clause is called when entering the json_returning_clause production.
	EnterJson_returning_clause(c *Json_returning_clauseContext)

	// EnterJson_predicate_type_constraint is called when entering the json_predicate_type_constraint production.
	EnterJson_predicate_type_constraint(c *Json_predicate_type_constraintContext)

	// EnterJson_key_uniqueness_constraint is called when entering the json_key_uniqueness_constraint production.
	EnterJson_key_uniqueness_constraint(c *Json_key_uniqueness_constraintContext)

	// EnterJson_name_and_value_list is called when entering the json_name_and_value_list production.
	EnterJson_name_and_value_list(c *Json_name_and_value_listContext)

	// EnterJson_name_and_value is called when entering the json_name_and_value production.
	EnterJson_name_and_value(c *Json_name_and_valueContext)

	// EnterJson_object_constructor_null_clause is called when entering the json_object_constructor_null_clause production.
	EnterJson_object_constructor_null_clause(c *Json_object_constructor_null_clauseContext)

	// EnterJson_array_constructor_null_clause is called when entering the json_array_constructor_null_clause production.
	EnterJson_array_constructor_null_clause(c *Json_array_constructor_null_clauseContext)

	// EnterJson_value_expr_list is called when entering the json_value_expr_list production.
	EnterJson_value_expr_list(c *Json_value_expr_listContext)

	// EnterJson_aggregate_func is called when entering the json_aggregate_func production.
	EnterJson_aggregate_func(c *Json_aggregate_funcContext)

	// EnterJson_array_aggregate_order_by_clause is called when entering the json_array_aggregate_order_by_clause production.
	EnterJson_array_aggregate_order_by_clause(c *Json_array_aggregate_order_by_clauseContext)

	// EnterTarget_list_ is called when entering the target_list_ production.
	EnterTarget_list_(c *Target_list_Context)

	// EnterTarget_list is called when entering the target_list production.
	EnterTarget_list(c *Target_listContext)

	// EnterTarget_label is called when entering the target_label production.
	EnterTarget_label(c *Target_labelContext)

	// EnterTarget_star is called when entering the target_star production.
	EnterTarget_star(c *Target_starContext)

	// EnterQualified_name_list is called when entering the qualified_name_list production.
	EnterQualified_name_list(c *Qualified_name_listContext)

	// EnterQualified_name is called when entering the qualified_name production.
	EnterQualified_name(c *Qualified_nameContext)

	// EnterName_list is called when entering the name_list production.
	EnterName_list(c *Name_listContext)

	// EnterName is called when entering the name production.
	EnterName(c *NameContext)

	// EnterAttr_name is called when entering the attr_name production.
	EnterAttr_name(c *Attr_nameContext)

	// EnterFile_name is called when entering the file_name production.
	EnterFile_name(c *File_nameContext)

	// EnterFunc_name is called when entering the func_name production.
	EnterFunc_name(c *Func_nameContext)

	// EnterAexprconst is called when entering the aexprconst production.
	EnterAexprconst(c *AexprconstContext)

	// EnterXconst is called when entering the xconst production.
	EnterXconst(c *XconstContext)

	// EnterBconst is called when entering the bconst production.
	EnterBconst(c *BconstContext)

	// EnterFconst is called when entering the fconst production.
	EnterFconst(c *FconstContext)

	// EnterIconst is called when entering the iconst production.
	EnterIconst(c *IconstContext)

	// EnterSconst is called when entering the sconst production.
	EnterSconst(c *SconstContext)

	// EnterAnysconst is called when entering the anysconst production.
	EnterAnysconst(c *AnysconstContext)

	// EnterUescape_ is called when entering the uescape_ production.
	EnterUescape_(c *Uescape_Context)

	// EnterSignediconst is called when entering the signediconst production.
	EnterSignediconst(c *SignediconstContext)

	// EnterRoleid is called when entering the roleid production.
	EnterRoleid(c *RoleidContext)

	// EnterRolespec is called when entering the rolespec production.
	EnterRolespec(c *RolespecContext)

	// EnterRole_list is called when entering the role_list production.
	EnterRole_list(c *Role_listContext)

	// EnterColid is called when entering the colid production.
	EnterColid(c *ColidContext)

	// EnterType_function_name is called when entering the type_function_name production.
	EnterType_function_name(c *Type_function_nameContext)

	// EnterNonreservedword is called when entering the nonreservedword production.
	EnterNonreservedword(c *NonreservedwordContext)

	// EnterColLabel is called when entering the colLabel production.
	EnterColLabel(c *ColLabelContext)

	// EnterBareColLabel is called when entering the bareColLabel production.
	EnterBareColLabel(c *BareColLabelContext)

	// EnterUnreserved_keyword is called when entering the unreserved_keyword production.
	EnterUnreserved_keyword(c *Unreserved_keywordContext)

	// EnterCol_name_keyword is called when entering the col_name_keyword production.
	EnterCol_name_keyword(c *Col_name_keywordContext)

	// EnterType_func_name_keyword is called when entering the type_func_name_keyword production.
	EnterType_func_name_keyword(c *Type_func_name_keywordContext)

	// EnterReserved_keyword is called when entering the reserved_keyword production.
	EnterReserved_keyword(c *Reserved_keywordContext)

	// EnterBare_label_keyword is called when entering the bare_label_keyword production.
	EnterBare_label_keyword(c *Bare_label_keywordContext)

	// EnterAny_identifier is called when entering the any_identifier production.
	EnterAny_identifier(c *Any_identifierContext)

	// EnterIdentifier is called when entering the identifier production.
	EnterIdentifier(c *IdentifierContext)

	// ExitRoot is called when exiting the root production.
	ExitRoot(c *RootContext)

	// ExitStmtblock is called when exiting the stmtblock production.
	ExitStmtblock(c *StmtblockContext)

	// ExitStmtmulti is called when exiting the stmtmulti production.
	ExitStmtmulti(c *StmtmultiContext)

	// ExitStmt is called when exiting the stmt production.
	ExitStmt(c *StmtContext)

	// ExitCallstmt is called when exiting the callstmt production.
	ExitCallstmt(c *CallstmtContext)

	// ExitCreaterolestmt is called when exiting the createrolestmt production.
	ExitCreaterolestmt(c *CreaterolestmtContext)

	// ExitWith_ is called when exiting the with_ production.
	ExitWith_(c *With_Context)

	// ExitOptrolelist is called when exiting the optrolelist production.
	ExitOptrolelist(c *OptrolelistContext)

	// ExitAlteroptrolelist is called when exiting the alteroptrolelist production.
	ExitAlteroptrolelist(c *AlteroptrolelistContext)

	// ExitAlteroptroleelem is called when exiting the alteroptroleelem production.
	ExitAlteroptroleelem(c *AlteroptroleelemContext)

	// ExitCreateoptroleelem is called when exiting the createoptroleelem production.
	ExitCreateoptroleelem(c *CreateoptroleelemContext)

	// ExitCreateuserstmt is called when exiting the createuserstmt production.
	ExitCreateuserstmt(c *CreateuserstmtContext)

	// ExitAlterrolestmt is called when exiting the alterrolestmt production.
	ExitAlterrolestmt(c *AlterrolestmtContext)

	// ExitIn_database_ is called when exiting the in_database_ production.
	ExitIn_database_(c *In_database_Context)

	// ExitAlterrolesetstmt is called when exiting the alterrolesetstmt production.
	ExitAlterrolesetstmt(c *AlterrolesetstmtContext)

	// ExitDroprolestmt is called when exiting the droprolestmt production.
	ExitDroprolestmt(c *DroprolestmtContext)

	// ExitCreategroupstmt is called when exiting the creategroupstmt production.
	ExitCreategroupstmt(c *CreategroupstmtContext)

	// ExitAltergroupstmt is called when exiting the altergroupstmt production.
	ExitAltergroupstmt(c *AltergroupstmtContext)

	// ExitAdd_drop is called when exiting the add_drop production.
	ExitAdd_drop(c *Add_dropContext)

	// ExitCreateschemastmt is called when exiting the createschemastmt production.
	ExitCreateschemastmt(c *CreateschemastmtContext)

	// ExitOptschemaname is called when exiting the optschemaname production.
	ExitOptschemaname(c *OptschemanameContext)

	// ExitOptschemaeltlist is called when exiting the optschemaeltlist production.
	ExitOptschemaeltlist(c *OptschemaeltlistContext)

	// ExitSchema_stmt is called when exiting the schema_stmt production.
	ExitSchema_stmt(c *Schema_stmtContext)

	// ExitVariablesetstmt is called when exiting the variablesetstmt production.
	ExitVariablesetstmt(c *VariablesetstmtContext)

	// ExitSet_rest is called when exiting the set_rest production.
	ExitSet_rest(c *Set_restContext)

	// ExitGeneric_set is called when exiting the generic_set production.
	ExitGeneric_set(c *Generic_setContext)

	// ExitSet_rest_more is called when exiting the set_rest_more production.
	ExitSet_rest_more(c *Set_rest_moreContext)

	// ExitVar_name is called when exiting the var_name production.
	ExitVar_name(c *Var_nameContext)

	// ExitVar_list is called when exiting the var_list production.
	ExitVar_list(c *Var_listContext)

	// ExitVar_value is called when exiting the var_value production.
	ExitVar_value(c *Var_valueContext)

	// ExitIso_level is called when exiting the iso_level production.
	ExitIso_level(c *Iso_levelContext)

	// ExitBoolean_or_string_ is called when exiting the boolean_or_string_ production.
	ExitBoolean_or_string_(c *Boolean_or_string_Context)

	// ExitZone_value is called when exiting the zone_value production.
	ExitZone_value(c *Zone_valueContext)

	// ExitEncoding_ is called when exiting the encoding_ production.
	ExitEncoding_(c *Encoding_Context)

	// ExitNonreservedword_or_sconst is called when exiting the nonreservedword_or_sconst production.
	ExitNonreservedword_or_sconst(c *Nonreservedword_or_sconstContext)

	// ExitVariableresetstmt is called when exiting the variableresetstmt production.
	ExitVariableresetstmt(c *VariableresetstmtContext)

	// ExitReset_rest is called when exiting the reset_rest production.
	ExitReset_rest(c *Reset_restContext)

	// ExitGeneric_reset is called when exiting the generic_reset production.
	ExitGeneric_reset(c *Generic_resetContext)

	// ExitSetresetclause is called when exiting the setresetclause production.
	ExitSetresetclause(c *SetresetclauseContext)

	// ExitFunctionsetresetclause is called when exiting the functionsetresetclause production.
	ExitFunctionsetresetclause(c *FunctionsetresetclauseContext)

	// ExitVariableshowstmt is called when exiting the variableshowstmt production.
	ExitVariableshowstmt(c *VariableshowstmtContext)

	// ExitConstraintssetstmt is called when exiting the constraintssetstmt production.
	ExitConstraintssetstmt(c *ConstraintssetstmtContext)

	// ExitConstraints_set_list is called when exiting the constraints_set_list production.
	ExitConstraints_set_list(c *Constraints_set_listContext)

	// ExitConstraints_set_mode is called when exiting the constraints_set_mode production.
	ExitConstraints_set_mode(c *Constraints_set_modeContext)

	// ExitCheckpointstmt is called when exiting the checkpointstmt production.
	ExitCheckpointstmt(c *CheckpointstmtContext)

	// ExitDiscardstmt is called when exiting the discardstmt production.
	ExitDiscardstmt(c *DiscardstmtContext)

	// ExitAltertablestmt is called when exiting the altertablestmt production.
	ExitAltertablestmt(c *AltertablestmtContext)

	// ExitAlter_table_cmds is called when exiting the alter_table_cmds production.
	ExitAlter_table_cmds(c *Alter_table_cmdsContext)

	// ExitPartition_cmd is called when exiting the partition_cmd production.
	ExitPartition_cmd(c *Partition_cmdContext)

	// ExitIndex_partition_cmd is called when exiting the index_partition_cmd production.
	ExitIndex_partition_cmd(c *Index_partition_cmdContext)

	// ExitAlter_table_cmd is called when exiting the alter_table_cmd production.
	ExitAlter_table_cmd(c *Alter_table_cmdContext)

	// ExitAlter_column_default is called when exiting the alter_column_default production.
	ExitAlter_column_default(c *Alter_column_defaultContext)

	// ExitDrop_behavior_ is called when exiting the drop_behavior_ production.
	ExitDrop_behavior_(c *Drop_behavior_Context)

	// ExitCollate_clause_ is called when exiting the collate_clause_ production.
	ExitCollate_clause_(c *Collate_clause_Context)

	// ExitAlter_using is called when exiting the alter_using production.
	ExitAlter_using(c *Alter_usingContext)

	// ExitReplica_identity is called when exiting the replica_identity production.
	ExitReplica_identity(c *Replica_identityContext)

	// ExitReloptions is called when exiting the reloptions production.
	ExitReloptions(c *ReloptionsContext)

	// ExitReloptions_ is called when exiting the reloptions_ production.
	ExitReloptions_(c *Reloptions_Context)

	// ExitReloption_list is called when exiting the reloption_list production.
	ExitReloption_list(c *Reloption_listContext)

	// ExitReloption_elem is called when exiting the reloption_elem production.
	ExitReloption_elem(c *Reloption_elemContext)

	// ExitAlter_identity_column_option_list is called when exiting the alter_identity_column_option_list production.
	ExitAlter_identity_column_option_list(c *Alter_identity_column_option_listContext)

	// ExitAlter_identity_column_option is called when exiting the alter_identity_column_option production.
	ExitAlter_identity_column_option(c *Alter_identity_column_optionContext)

	// ExitPartitionboundspec is called when exiting the partitionboundspec production.
	ExitPartitionboundspec(c *PartitionboundspecContext)

	// ExitHash_partbound_elem is called when exiting the hash_partbound_elem production.
	ExitHash_partbound_elem(c *Hash_partbound_elemContext)

	// ExitHash_partbound is called when exiting the hash_partbound production.
	ExitHash_partbound(c *Hash_partboundContext)

	// ExitAltercompositetypestmt is called when exiting the altercompositetypestmt production.
	ExitAltercompositetypestmt(c *AltercompositetypestmtContext)

	// ExitAlter_type_cmds is called when exiting the alter_type_cmds production.
	ExitAlter_type_cmds(c *Alter_type_cmdsContext)

	// ExitAlter_type_cmd is called when exiting the alter_type_cmd production.
	ExitAlter_type_cmd(c *Alter_type_cmdContext)

	// ExitCloseportalstmt is called when exiting the closeportalstmt production.
	ExitCloseportalstmt(c *CloseportalstmtContext)

	// ExitCopystmt is called when exiting the copystmt production.
	ExitCopystmt(c *CopystmtContext)

	// ExitCopy_from is called when exiting the copy_from production.
	ExitCopy_from(c *Copy_fromContext)

	// ExitProgram_ is called when exiting the program_ production.
	ExitProgram_(c *Program_Context)

	// ExitCopy_file_name is called when exiting the copy_file_name production.
	ExitCopy_file_name(c *Copy_file_nameContext)

	// ExitCopy_options is called when exiting the copy_options production.
	ExitCopy_options(c *Copy_optionsContext)

	// ExitCopy_opt_list is called when exiting the copy_opt_list production.
	ExitCopy_opt_list(c *Copy_opt_listContext)

	// ExitCopy_opt_item is called when exiting the copy_opt_item production.
	ExitCopy_opt_item(c *Copy_opt_itemContext)

	// ExitBinary_ is called when exiting the binary_ production.
	ExitBinary_(c *Binary_Context)

	// ExitCopy_delimiter is called when exiting the copy_delimiter production.
	ExitCopy_delimiter(c *Copy_delimiterContext)

	// ExitUsing_ is called when exiting the using_ production.
	ExitUsing_(c *Using_Context)

	// ExitCopy_generic_opt_list is called when exiting the copy_generic_opt_list production.
	ExitCopy_generic_opt_list(c *Copy_generic_opt_listContext)

	// ExitCopy_generic_opt_elem is called when exiting the copy_generic_opt_elem production.
	ExitCopy_generic_opt_elem(c *Copy_generic_opt_elemContext)

	// ExitCopy_generic_opt_arg is called when exiting the copy_generic_opt_arg production.
	ExitCopy_generic_opt_arg(c *Copy_generic_opt_argContext)

	// ExitCopy_generic_opt_arg_list is called when exiting the copy_generic_opt_arg_list production.
	ExitCopy_generic_opt_arg_list(c *Copy_generic_opt_arg_listContext)

	// ExitCopy_generic_opt_arg_list_item is called when exiting the copy_generic_opt_arg_list_item production.
	ExitCopy_generic_opt_arg_list_item(c *Copy_generic_opt_arg_list_itemContext)

	// ExitCreatestmt is called when exiting the createstmt production.
	ExitCreatestmt(c *CreatestmtContext)

	// ExitOpttemp is called when exiting the opttemp production.
	ExitOpttemp(c *OpttempContext)

	// ExitOpttableelementlist is called when exiting the opttableelementlist production.
	ExitOpttableelementlist(c *OpttableelementlistContext)

	// ExitOpttypedtableelementlist is called when exiting the opttypedtableelementlist production.
	ExitOpttypedtableelementlist(c *OpttypedtableelementlistContext)

	// ExitTableelementlist is called when exiting the tableelementlist production.
	ExitTableelementlist(c *TableelementlistContext)

	// ExitTypedtableelementlist is called when exiting the typedtableelementlist production.
	ExitTypedtableelementlist(c *TypedtableelementlistContext)

	// ExitTableelement is called when exiting the tableelement production.
	ExitTableelement(c *TableelementContext)

	// ExitTypedtableelement is called when exiting the typedtableelement production.
	ExitTypedtableelement(c *TypedtableelementContext)

	// ExitColumnDef is called when exiting the columnDef production.
	ExitColumnDef(c *ColumnDefContext)

	// ExitColumnOptions is called when exiting the columnOptions production.
	ExitColumnOptions(c *ColumnOptionsContext)

	// ExitColquallist is called when exiting the colquallist production.
	ExitColquallist(c *ColquallistContext)

	// ExitColconstraint is called when exiting the colconstraint production.
	ExitColconstraint(c *ColconstraintContext)

	// ExitColconstraintelem is called when exiting the colconstraintelem production.
	ExitColconstraintelem(c *ColconstraintelemContext)

	// ExitGenerated_when is called when exiting the generated_when production.
	ExitGenerated_when(c *Generated_whenContext)

	// ExitConstraintattr is called when exiting the constraintattr production.
	ExitConstraintattr(c *ConstraintattrContext)

	// ExitTablelikeclause is called when exiting the tablelikeclause production.
	ExitTablelikeclause(c *TablelikeclauseContext)

	// ExitTablelikeoptionlist is called when exiting the tablelikeoptionlist production.
	ExitTablelikeoptionlist(c *TablelikeoptionlistContext)

	// ExitTablelikeoption is called when exiting the tablelikeoption production.
	ExitTablelikeoption(c *TablelikeoptionContext)

	// ExitTableconstraint is called when exiting the tableconstraint production.
	ExitTableconstraint(c *TableconstraintContext)

	// ExitConstraintelem is called when exiting the constraintelem production.
	ExitConstraintelem(c *ConstraintelemContext)

	// ExitNo_inherit_ is called when exiting the no_inherit_ production.
	ExitNo_inherit_(c *No_inherit_Context)

	// ExitColumn_list_ is called when exiting the column_list_ production.
	ExitColumn_list_(c *Column_list_Context)

	// ExitColumnlist is called when exiting the columnlist production.
	ExitColumnlist(c *ColumnlistContext)

	// ExitColumnElem is called when exiting the columnElem production.
	ExitColumnElem(c *ColumnElemContext)

	// ExitC_include_ is called when exiting the c_include_ production.
	ExitC_include_(c *C_include_Context)

	// ExitKey_match is called when exiting the key_match production.
	ExitKey_match(c *Key_matchContext)

	// ExitExclusionconstraintlist is called when exiting the exclusionconstraintlist production.
	ExitExclusionconstraintlist(c *ExclusionconstraintlistContext)

	// ExitExclusionconstraintelem is called when exiting the exclusionconstraintelem production.
	ExitExclusionconstraintelem(c *ExclusionconstraintelemContext)

	// ExitExclusionwhereclause is called when exiting the exclusionwhereclause production.
	ExitExclusionwhereclause(c *ExclusionwhereclauseContext)

	// ExitKey_actions is called when exiting the key_actions production.
	ExitKey_actions(c *Key_actionsContext)

	// ExitKey_update is called when exiting the key_update production.
	ExitKey_update(c *Key_updateContext)

	// ExitKey_delete is called when exiting the key_delete production.
	ExitKey_delete(c *Key_deleteContext)

	// ExitKey_action is called when exiting the key_action production.
	ExitKey_action(c *Key_actionContext)

	// ExitOptinherit is called when exiting the optinherit production.
	ExitOptinherit(c *OptinheritContext)

	// ExitOptpartitionspec is called when exiting the optpartitionspec production.
	ExitOptpartitionspec(c *OptpartitionspecContext)

	// ExitPartitionspec is called when exiting the partitionspec production.
	ExitPartitionspec(c *PartitionspecContext)

	// ExitPart_params is called when exiting the part_params production.
	ExitPart_params(c *Part_paramsContext)

	// ExitPart_elem is called when exiting the part_elem production.
	ExitPart_elem(c *Part_elemContext)

	// ExitTable_access_method_clause is called when exiting the table_access_method_clause production.
	ExitTable_access_method_clause(c *Table_access_method_clauseContext)

	// ExitOptwith is called when exiting the optwith production.
	ExitOptwith(c *OptwithContext)

	// ExitOncommitoption is called when exiting the oncommitoption production.
	ExitOncommitoption(c *OncommitoptionContext)

	// ExitOpttablespace is called when exiting the opttablespace production.
	ExitOpttablespace(c *OpttablespaceContext)

	// ExitOptconstablespace is called when exiting the optconstablespace production.
	ExitOptconstablespace(c *OptconstablespaceContext)

	// ExitExistingindex is called when exiting the existingindex production.
	ExitExistingindex(c *ExistingindexContext)

	// ExitCreatestatsstmt is called when exiting the createstatsstmt production.
	ExitCreatestatsstmt(c *CreatestatsstmtContext)

	// ExitAlterstatsstmt is called when exiting the alterstatsstmt production.
	ExitAlterstatsstmt(c *AlterstatsstmtContext)

	// ExitCreateasstmt is called when exiting the createasstmt production.
	ExitCreateasstmt(c *CreateasstmtContext)

	// ExitCreate_as_target is called when exiting the create_as_target production.
	ExitCreate_as_target(c *Create_as_targetContext)

	// ExitWith_data_ is called when exiting the with_data_ production.
	ExitWith_data_(c *With_data_Context)

	// ExitCreatematviewstmt is called when exiting the creatematviewstmt production.
	ExitCreatematviewstmt(c *CreatematviewstmtContext)

	// ExitCreate_mv_target is called when exiting the create_mv_target production.
	ExitCreate_mv_target(c *Create_mv_targetContext)

	// ExitOptnolog is called when exiting the optnolog production.
	ExitOptnolog(c *OptnologContext)

	// ExitRefreshmatviewstmt is called when exiting the refreshmatviewstmt production.
	ExitRefreshmatviewstmt(c *RefreshmatviewstmtContext)

	// ExitCreateseqstmt is called when exiting the createseqstmt production.
	ExitCreateseqstmt(c *CreateseqstmtContext)

	// ExitAlterseqstmt is called when exiting the alterseqstmt production.
	ExitAlterseqstmt(c *AlterseqstmtContext)

	// ExitOptseqoptlist is called when exiting the optseqoptlist production.
	ExitOptseqoptlist(c *OptseqoptlistContext)

	// ExitOptparenthesizedseqoptlist is called when exiting the optparenthesizedseqoptlist production.
	ExitOptparenthesizedseqoptlist(c *OptparenthesizedseqoptlistContext)

	// ExitSeqoptlist is called when exiting the seqoptlist production.
	ExitSeqoptlist(c *SeqoptlistContext)

	// ExitSeqoptelem is called when exiting the seqoptelem production.
	ExitSeqoptelem(c *SeqoptelemContext)

	// ExitBy_ is called when exiting the by_ production.
	ExitBy_(c *By_Context)

	// ExitNumericonly is called when exiting the numericonly production.
	ExitNumericonly(c *NumericonlyContext)

	// ExitNumericonly_list is called when exiting the numericonly_list production.
	ExitNumericonly_list(c *Numericonly_listContext)

	// ExitCreateplangstmt is called when exiting the createplangstmt production.
	ExitCreateplangstmt(c *CreateplangstmtContext)

	// ExitTrusted_ is called when exiting the trusted_ production.
	ExitTrusted_(c *Trusted_Context)

	// ExitHandler_name is called when exiting the handler_name production.
	ExitHandler_name(c *Handler_nameContext)

	// ExitInline_handler_ is called when exiting the inline_handler_ production.
	ExitInline_handler_(c *Inline_handler_Context)

	// ExitValidator_clause is called when exiting the validator_clause production.
	ExitValidator_clause(c *Validator_clauseContext)

	// ExitValidator_ is called when exiting the validator_ production.
	ExitValidator_(c *Validator_Context)

	// ExitProcedural_ is called when exiting the procedural_ production.
	ExitProcedural_(c *Procedural_Context)

	// ExitCreatetablespacestmt is called when exiting the createtablespacestmt production.
	ExitCreatetablespacestmt(c *CreatetablespacestmtContext)

	// ExitOpttablespaceowner is called when exiting the opttablespaceowner production.
	ExitOpttablespaceowner(c *OpttablespaceownerContext)

	// ExitDroptablespacestmt is called when exiting the droptablespacestmt production.
	ExitDroptablespacestmt(c *DroptablespacestmtContext)

	// ExitCreateextensionstmt is called when exiting the createextensionstmt production.
	ExitCreateextensionstmt(c *CreateextensionstmtContext)

	// ExitCreate_extension_opt_list is called when exiting the create_extension_opt_list production.
	ExitCreate_extension_opt_list(c *Create_extension_opt_listContext)

	// ExitCreate_extension_opt_item is called when exiting the create_extension_opt_item production.
	ExitCreate_extension_opt_item(c *Create_extension_opt_itemContext)

	// ExitAlterextensionstmt is called when exiting the alterextensionstmt production.
	ExitAlterextensionstmt(c *AlterextensionstmtContext)

	// ExitAlter_extension_opt_list is called when exiting the alter_extension_opt_list production.
	ExitAlter_extension_opt_list(c *Alter_extension_opt_listContext)

	// ExitAlter_extension_opt_item is called when exiting the alter_extension_opt_item production.
	ExitAlter_extension_opt_item(c *Alter_extension_opt_itemContext)

	// ExitAlterextensioncontentsstmt is called when exiting the alterextensioncontentsstmt production.
	ExitAlterextensioncontentsstmt(c *AlterextensioncontentsstmtContext)

	// ExitCreatefdwstmt is called when exiting the createfdwstmt production.
	ExitCreatefdwstmt(c *CreatefdwstmtContext)

	// ExitFdw_option is called when exiting the fdw_option production.
	ExitFdw_option(c *Fdw_optionContext)

	// ExitFdw_options is called when exiting the fdw_options production.
	ExitFdw_options(c *Fdw_optionsContext)

	// ExitFdw_options_ is called when exiting the fdw_options_ production.
	ExitFdw_options_(c *Fdw_options_Context)

	// ExitAlterfdwstmt is called when exiting the alterfdwstmt production.
	ExitAlterfdwstmt(c *AlterfdwstmtContext)

	// ExitCreate_generic_options is called when exiting the create_generic_options production.
	ExitCreate_generic_options(c *Create_generic_optionsContext)

	// ExitGeneric_option_list is called when exiting the generic_option_list production.
	ExitGeneric_option_list(c *Generic_option_listContext)

	// ExitAlter_generic_options is called when exiting the alter_generic_options production.
	ExitAlter_generic_options(c *Alter_generic_optionsContext)

	// ExitAlter_generic_option_list is called when exiting the alter_generic_option_list production.
	ExitAlter_generic_option_list(c *Alter_generic_option_listContext)

	// ExitAlter_generic_option_elem is called when exiting the alter_generic_option_elem production.
	ExitAlter_generic_option_elem(c *Alter_generic_option_elemContext)

	// ExitGeneric_option_elem is called when exiting the generic_option_elem production.
	ExitGeneric_option_elem(c *Generic_option_elemContext)

	// ExitGeneric_option_name is called when exiting the generic_option_name production.
	ExitGeneric_option_name(c *Generic_option_nameContext)

	// ExitGeneric_option_arg is called when exiting the generic_option_arg production.
	ExitGeneric_option_arg(c *Generic_option_argContext)

	// ExitCreateforeignserverstmt is called when exiting the createforeignserverstmt production.
	ExitCreateforeignserverstmt(c *CreateforeignserverstmtContext)

	// ExitType_ is called when exiting the type_ production.
	ExitType_(c *Type_Context)

	// ExitForeign_server_version is called when exiting the foreign_server_version production.
	ExitForeign_server_version(c *Foreign_server_versionContext)

	// ExitForeign_server_version_ is called when exiting the foreign_server_version_ production.
	ExitForeign_server_version_(c *Foreign_server_version_Context)

	// ExitAlterforeignserverstmt is called when exiting the alterforeignserverstmt production.
	ExitAlterforeignserverstmt(c *AlterforeignserverstmtContext)

	// ExitCreateforeigntablestmt is called when exiting the createforeigntablestmt production.
	ExitCreateforeigntablestmt(c *CreateforeigntablestmtContext)

	// ExitImportforeignschemastmt is called when exiting the importforeignschemastmt production.
	ExitImportforeignschemastmt(c *ImportforeignschemastmtContext)

	// ExitImport_qualification_type is called when exiting the import_qualification_type production.
	ExitImport_qualification_type(c *Import_qualification_typeContext)

	// ExitImport_qualification is called when exiting the import_qualification production.
	ExitImport_qualification(c *Import_qualificationContext)

	// ExitCreateusermappingstmt is called when exiting the createusermappingstmt production.
	ExitCreateusermappingstmt(c *CreateusermappingstmtContext)

	// ExitAuth_ident is called when exiting the auth_ident production.
	ExitAuth_ident(c *Auth_identContext)

	// ExitDropusermappingstmt is called when exiting the dropusermappingstmt production.
	ExitDropusermappingstmt(c *DropusermappingstmtContext)

	// ExitAlterusermappingstmt is called when exiting the alterusermappingstmt production.
	ExitAlterusermappingstmt(c *AlterusermappingstmtContext)

	// ExitCreatepolicystmt is called when exiting the createpolicystmt production.
	ExitCreatepolicystmt(c *CreatepolicystmtContext)

	// ExitAlterpolicystmt is called when exiting the alterpolicystmt production.
	ExitAlterpolicystmt(c *AlterpolicystmtContext)

	// ExitRowsecurityoptionalexpr is called when exiting the rowsecurityoptionalexpr production.
	ExitRowsecurityoptionalexpr(c *RowsecurityoptionalexprContext)

	// ExitRowsecurityoptionalwithcheck is called when exiting the rowsecurityoptionalwithcheck production.
	ExitRowsecurityoptionalwithcheck(c *RowsecurityoptionalwithcheckContext)

	// ExitRowsecuritydefaulttorole is called when exiting the rowsecuritydefaulttorole production.
	ExitRowsecuritydefaulttorole(c *RowsecuritydefaulttoroleContext)

	// ExitRowsecurityoptionaltorole is called when exiting the rowsecurityoptionaltorole production.
	ExitRowsecurityoptionaltorole(c *RowsecurityoptionaltoroleContext)

	// ExitRowsecuritydefaultpermissive is called when exiting the rowsecuritydefaultpermissive production.
	ExitRowsecuritydefaultpermissive(c *RowsecuritydefaultpermissiveContext)

	// ExitRowsecuritydefaultforcmd is called when exiting the rowsecuritydefaultforcmd production.
	ExitRowsecuritydefaultforcmd(c *RowsecuritydefaultforcmdContext)

	// ExitRow_security_cmd is called when exiting the row_security_cmd production.
	ExitRow_security_cmd(c *Row_security_cmdContext)

	// ExitCreateamstmt is called when exiting the createamstmt production.
	ExitCreateamstmt(c *CreateamstmtContext)

	// ExitAm_type is called when exiting the am_type production.
	ExitAm_type(c *Am_typeContext)

	// ExitCreatetrigstmt is called when exiting the createtrigstmt production.
	ExitCreatetrigstmt(c *CreatetrigstmtContext)

	// ExitTriggeractiontime is called when exiting the triggeractiontime production.
	ExitTriggeractiontime(c *TriggeractiontimeContext)

	// ExitTriggerevents is called when exiting the triggerevents production.
	ExitTriggerevents(c *TriggereventsContext)

	// ExitTriggeroneevent is called when exiting the triggeroneevent production.
	ExitTriggeroneevent(c *TriggeroneeventContext)

	// ExitTriggerreferencing is called when exiting the triggerreferencing production.
	ExitTriggerreferencing(c *TriggerreferencingContext)

	// ExitTriggertransitions is called when exiting the triggertransitions production.
	ExitTriggertransitions(c *TriggertransitionsContext)

	// ExitTriggertransition is called when exiting the triggertransition production.
	ExitTriggertransition(c *TriggertransitionContext)

	// ExitTransitionoldornew is called when exiting the transitionoldornew production.
	ExitTransitionoldornew(c *TransitionoldornewContext)

	// ExitTransitionrowortable is called when exiting the transitionrowortable production.
	ExitTransitionrowortable(c *TransitionrowortableContext)

	// ExitTransitionrelname is called when exiting the transitionrelname production.
	ExitTransitionrelname(c *TransitionrelnameContext)

	// ExitTriggerforspec is called when exiting the triggerforspec production.
	ExitTriggerforspec(c *TriggerforspecContext)

	// ExitTriggerforopteach is called when exiting the triggerforopteach production.
	ExitTriggerforopteach(c *TriggerforopteachContext)

	// ExitTriggerfortype is called when exiting the triggerfortype production.
	ExitTriggerfortype(c *TriggerfortypeContext)

	// ExitTriggerwhen is called when exiting the triggerwhen production.
	ExitTriggerwhen(c *TriggerwhenContext)

	// ExitFunction_or_procedure is called when exiting the function_or_procedure production.
	ExitFunction_or_procedure(c *Function_or_procedureContext)

	// ExitTriggerfuncargs is called when exiting the triggerfuncargs production.
	ExitTriggerfuncargs(c *TriggerfuncargsContext)

	// ExitTriggerfuncarg is called when exiting the triggerfuncarg production.
	ExitTriggerfuncarg(c *TriggerfuncargContext)

	// ExitOptconstrfromtable is called when exiting the optconstrfromtable production.
	ExitOptconstrfromtable(c *OptconstrfromtableContext)

	// ExitConstraintattributespec is called when exiting the constraintattributespec production.
	ExitConstraintattributespec(c *ConstraintattributespecContext)

	// ExitConstraintattributeElem is called when exiting the constraintattributeElem production.
	ExitConstraintattributeElem(c *ConstraintattributeElemContext)

	// ExitCreateeventtrigstmt is called when exiting the createeventtrigstmt production.
	ExitCreateeventtrigstmt(c *CreateeventtrigstmtContext)

	// ExitEvent_trigger_when_list is called when exiting the event_trigger_when_list production.
	ExitEvent_trigger_when_list(c *Event_trigger_when_listContext)

	// ExitEvent_trigger_when_item is called when exiting the event_trigger_when_item production.
	ExitEvent_trigger_when_item(c *Event_trigger_when_itemContext)

	// ExitEvent_trigger_value_list is called when exiting the event_trigger_value_list production.
	ExitEvent_trigger_value_list(c *Event_trigger_value_listContext)

	// ExitAltereventtrigstmt is called when exiting the altereventtrigstmt production.
	ExitAltereventtrigstmt(c *AltereventtrigstmtContext)

	// ExitEnable_trigger is called when exiting the enable_trigger production.
	ExitEnable_trigger(c *Enable_triggerContext)

	// ExitCreateassertionstmt is called when exiting the createassertionstmt production.
	ExitCreateassertionstmt(c *CreateassertionstmtContext)

	// ExitDefinestmt is called when exiting the definestmt production.
	ExitDefinestmt(c *DefinestmtContext)

	// ExitDefinition is called when exiting the definition production.
	ExitDefinition(c *DefinitionContext)

	// ExitDef_list is called when exiting the def_list production.
	ExitDef_list(c *Def_listContext)

	// ExitDef_elem is called when exiting the def_elem production.
	ExitDef_elem(c *Def_elemContext)

	// ExitDef_arg is called when exiting the def_arg production.
	ExitDef_arg(c *Def_argContext)

	// ExitOld_aggr_definition is called when exiting the old_aggr_definition production.
	ExitOld_aggr_definition(c *Old_aggr_definitionContext)

	// ExitOld_aggr_list is called when exiting the old_aggr_list production.
	ExitOld_aggr_list(c *Old_aggr_listContext)

	// ExitOld_aggr_elem is called when exiting the old_aggr_elem production.
	ExitOld_aggr_elem(c *Old_aggr_elemContext)

	// ExitEnum_val_list_ is called when exiting the enum_val_list_ production.
	ExitEnum_val_list_(c *Enum_val_list_Context)

	// ExitEnum_val_list is called when exiting the enum_val_list production.
	ExitEnum_val_list(c *Enum_val_listContext)

	// ExitAlterenumstmt is called when exiting the alterenumstmt production.
	ExitAlterenumstmt(c *AlterenumstmtContext)

	// ExitIf_not_exists_ is called when exiting the if_not_exists_ production.
	ExitIf_not_exists_(c *If_not_exists_Context)

	// ExitCreateopclassstmt is called when exiting the createopclassstmt production.
	ExitCreateopclassstmt(c *CreateopclassstmtContext)

	// ExitOpclass_item_list is called when exiting the opclass_item_list production.
	ExitOpclass_item_list(c *Opclass_item_listContext)

	// ExitOpclass_item is called when exiting the opclass_item production.
	ExitOpclass_item(c *Opclass_itemContext)

	// ExitDefault_ is called when exiting the default_ production.
	ExitDefault_(c *Default_Context)

	// ExitOpfamily_ is called when exiting the opfamily_ production.
	ExitOpfamily_(c *Opfamily_Context)

	// ExitOpclass_purpose is called when exiting the opclass_purpose production.
	ExitOpclass_purpose(c *Opclass_purposeContext)

	// ExitRecheck_ is called when exiting the recheck_ production.
	ExitRecheck_(c *Recheck_Context)

	// ExitCreateopfamilystmt is called when exiting the createopfamilystmt production.
	ExitCreateopfamilystmt(c *CreateopfamilystmtContext)

	// ExitAlteropfamilystmt is called when exiting the alteropfamilystmt production.
	ExitAlteropfamilystmt(c *AlteropfamilystmtContext)

	// ExitOpclass_drop_list is called when exiting the opclass_drop_list production.
	ExitOpclass_drop_list(c *Opclass_drop_listContext)

	// ExitOpclass_drop is called when exiting the opclass_drop production.
	ExitOpclass_drop(c *Opclass_dropContext)

	// ExitDropopclassstmt is called when exiting the dropopclassstmt production.
	ExitDropopclassstmt(c *DropopclassstmtContext)

	// ExitDropopfamilystmt is called when exiting the dropopfamilystmt production.
	ExitDropopfamilystmt(c *DropopfamilystmtContext)

	// ExitDropownedstmt is called when exiting the dropownedstmt production.
	ExitDropownedstmt(c *DropownedstmtContext)

	// ExitReassignownedstmt is called when exiting the reassignownedstmt production.
	ExitReassignownedstmt(c *ReassignownedstmtContext)

	// ExitDropstmt is called when exiting the dropstmt production.
	ExitDropstmt(c *DropstmtContext)

	// ExitObject_type_any_name is called when exiting the object_type_any_name production.
	ExitObject_type_any_name(c *Object_type_any_nameContext)

	// ExitObject_type_name is called when exiting the object_type_name production.
	ExitObject_type_name(c *Object_type_nameContext)

	// ExitDrop_type_name is called when exiting the drop_type_name production.
	ExitDrop_type_name(c *Drop_type_nameContext)

	// ExitObject_type_name_on_any_name is called when exiting the object_type_name_on_any_name production.
	ExitObject_type_name_on_any_name(c *Object_type_name_on_any_nameContext)

	// ExitAny_name_list_ is called when exiting the any_name_list_ production.
	ExitAny_name_list_(c *Any_name_list_Context)

	// ExitAny_name is called when exiting the any_name production.
	ExitAny_name(c *Any_nameContext)

	// ExitAttrs is called when exiting the attrs production.
	ExitAttrs(c *AttrsContext)

	// ExitType_name_list is called when exiting the type_name_list production.
	ExitType_name_list(c *Type_name_listContext)

	// ExitTruncatestmt is called when exiting the truncatestmt production.
	ExitTruncatestmt(c *TruncatestmtContext)

	// ExitRestart_seqs_ is called when exiting the restart_seqs_ production.
	ExitRestart_seqs_(c *Restart_seqs_Context)

	// ExitCommentstmt is called when exiting the commentstmt production.
	ExitCommentstmt(c *CommentstmtContext)

	// ExitComment_text is called when exiting the comment_text production.
	ExitComment_text(c *Comment_textContext)

	// ExitSeclabelstmt is called when exiting the seclabelstmt production.
	ExitSeclabelstmt(c *SeclabelstmtContext)

	// ExitProvider_ is called when exiting the provider_ production.
	ExitProvider_(c *Provider_Context)

	// ExitSecurity_label is called when exiting the security_label production.
	ExitSecurity_label(c *Security_labelContext)

	// ExitFetchstmt is called when exiting the fetchstmt production.
	ExitFetchstmt(c *FetchstmtContext)

	// ExitFetch_args is called when exiting the fetch_args production.
	ExitFetch_args(c *Fetch_argsContext)

	// ExitFrom_in is called when exiting the from_in production.
	ExitFrom_in(c *From_inContext)

	// ExitFrom_in_ is called when exiting the from_in_ production.
	ExitFrom_in_(c *From_in_Context)

	// ExitGrantstmt is called when exiting the grantstmt production.
	ExitGrantstmt(c *GrantstmtContext)

	// ExitRevokestmt is called when exiting the revokestmt production.
	ExitRevokestmt(c *RevokestmtContext)

	// ExitPrivileges is called when exiting the privileges production.
	ExitPrivileges(c *PrivilegesContext)

	// ExitPrivilege_list is called when exiting the privilege_list production.
	ExitPrivilege_list(c *Privilege_listContext)

	// ExitPrivilege is called when exiting the privilege production.
	ExitPrivilege(c *PrivilegeContext)

	// ExitPrivilege_target is called when exiting the privilege_target production.
	ExitPrivilege_target(c *Privilege_targetContext)

	// ExitGrantee_list is called when exiting the grantee_list production.
	ExitGrantee_list(c *Grantee_listContext)

	// ExitGrantee is called when exiting the grantee production.
	ExitGrantee(c *GranteeContext)

	// ExitGrant_grant_option_ is called when exiting the grant_grant_option_ production.
	ExitGrant_grant_option_(c *Grant_grant_option_Context)

	// ExitGrantrolestmt is called when exiting the grantrolestmt production.
	ExitGrantrolestmt(c *GrantrolestmtContext)

	// ExitRevokerolestmt is called when exiting the revokerolestmt production.
	ExitRevokerolestmt(c *RevokerolestmtContext)

	// ExitGrant_admin_option_ is called when exiting the grant_admin_option_ production.
	ExitGrant_admin_option_(c *Grant_admin_option_Context)

	// ExitGranted_by_ is called when exiting the granted_by_ production.
	ExitGranted_by_(c *Granted_by_Context)

	// ExitAlterdefaultprivilegesstmt is called when exiting the alterdefaultprivilegesstmt production.
	ExitAlterdefaultprivilegesstmt(c *AlterdefaultprivilegesstmtContext)

	// ExitDefacloptionlist is called when exiting the defacloptionlist production.
	ExitDefacloptionlist(c *DefacloptionlistContext)

	// ExitDefacloption is called when exiting the defacloption production.
	ExitDefacloption(c *DefacloptionContext)

	// ExitDefaclaction is called when exiting the defaclaction production.
	ExitDefaclaction(c *DefaclactionContext)

	// ExitDefacl_privilege_target is called when exiting the defacl_privilege_target production.
	ExitDefacl_privilege_target(c *Defacl_privilege_targetContext)

	// ExitIndexstmt is called when exiting the indexstmt production.
	ExitIndexstmt(c *IndexstmtContext)

	// ExitUnique_ is called when exiting the unique_ production.
	ExitUnique_(c *Unique_Context)

	// ExitSingle_name_ is called when exiting the single_name_ production.
	ExitSingle_name_(c *Single_name_Context)

	// ExitConcurrently_ is called when exiting the concurrently_ production.
	ExitConcurrently_(c *Concurrently_Context)

	// ExitIndex_name_ is called when exiting the index_name_ production.
	ExitIndex_name_(c *Index_name_Context)

	// ExitAccess_method_clause is called when exiting the access_method_clause production.
	ExitAccess_method_clause(c *Access_method_clauseContext)

	// ExitIndex_params is called when exiting the index_params production.
	ExitIndex_params(c *Index_paramsContext)

	// ExitIndex_elem_options is called when exiting the index_elem_options production.
	ExitIndex_elem_options(c *Index_elem_optionsContext)

	// ExitIndex_elem is called when exiting the index_elem production.
	ExitIndex_elem(c *Index_elemContext)

	// ExitInclude_ is called when exiting the include_ production.
	ExitInclude_(c *Include_Context)

	// ExitIndex_including_params is called when exiting the index_including_params production.
	ExitIndex_including_params(c *Index_including_paramsContext)

	// ExitCollate_ is called when exiting the collate_ production.
	ExitCollate_(c *Collate_Context)

	// ExitClass_ is called when exiting the class_ production.
	ExitClass_(c *Class_Context)

	// ExitAsc_desc_ is called when exiting the asc_desc_ production.
	ExitAsc_desc_(c *Asc_desc_Context)

	// ExitNulls_order_ is called when exiting the nulls_order_ production.
	ExitNulls_order_(c *Nulls_order_Context)

	// ExitCreatefunctionstmt is called when exiting the createfunctionstmt production.
	ExitCreatefunctionstmt(c *CreatefunctionstmtContext)

	// ExitOr_replace_ is called when exiting the or_replace_ production.
	ExitOr_replace_(c *Or_replace_Context)

	// ExitFunc_args is called when exiting the func_args production.
	ExitFunc_args(c *Func_argsContext)

	// ExitFunc_args_list is called when exiting the func_args_list production.
	ExitFunc_args_list(c *Func_args_listContext)

	// ExitFunction_with_argtypes_list is called when exiting the function_with_argtypes_list production.
	ExitFunction_with_argtypes_list(c *Function_with_argtypes_listContext)

	// ExitFunction_with_argtypes is called when exiting the function_with_argtypes production.
	ExitFunction_with_argtypes(c *Function_with_argtypesContext)

	// ExitFunc_args_with_defaults is called when exiting the func_args_with_defaults production.
	ExitFunc_args_with_defaults(c *Func_args_with_defaultsContext)

	// ExitFunc_args_with_defaults_list is called when exiting the func_args_with_defaults_list production.
	ExitFunc_args_with_defaults_list(c *Func_args_with_defaults_listContext)

	// ExitFunc_arg is called when exiting the func_arg production.
	ExitFunc_arg(c *Func_argContext)

	// ExitArg_class is called when exiting the arg_class production.
	ExitArg_class(c *Arg_classContext)

	// ExitParam_name is called when exiting the param_name production.
	ExitParam_name(c *Param_nameContext)

	// ExitFunc_return is called when exiting the func_return production.
	ExitFunc_return(c *Func_returnContext)

	// ExitFunc_type is called when exiting the func_type production.
	ExitFunc_type(c *Func_typeContext)

	// ExitFunc_arg_with_default is called when exiting the func_arg_with_default production.
	ExitFunc_arg_with_default(c *Func_arg_with_defaultContext)

	// ExitAggr_arg is called when exiting the aggr_arg production.
	ExitAggr_arg(c *Aggr_argContext)

	// ExitAggr_args is called when exiting the aggr_args production.
	ExitAggr_args(c *Aggr_argsContext)

	// ExitAggr_args_list is called when exiting the aggr_args_list production.
	ExitAggr_args_list(c *Aggr_args_listContext)

	// ExitAggregate_with_argtypes is called when exiting the aggregate_with_argtypes production.
	ExitAggregate_with_argtypes(c *Aggregate_with_argtypesContext)

	// ExitAggregate_with_argtypes_list is called when exiting the aggregate_with_argtypes_list production.
	ExitAggregate_with_argtypes_list(c *Aggregate_with_argtypes_listContext)

	// ExitCreatefunc_opt_list is called when exiting the createfunc_opt_list production.
	ExitCreatefunc_opt_list(c *Createfunc_opt_listContext)

	// ExitCommon_func_opt_item is called when exiting the common_func_opt_item production.
	ExitCommon_func_opt_item(c *Common_func_opt_itemContext)

	// ExitCreatefunc_opt_item is called when exiting the createfunc_opt_item production.
	ExitCreatefunc_opt_item(c *Createfunc_opt_itemContext)

	// ExitFunc_as is called when exiting the func_as production.
	ExitFunc_as(c *Func_asContext)

	// ExitTransform_type_list is called when exiting the transform_type_list production.
	ExitTransform_type_list(c *Transform_type_listContext)

	// ExitDefinition_ is called when exiting the definition_ production.
	ExitDefinition_(c *Definition_Context)

	// ExitTable_func_column is called when exiting the table_func_column production.
	ExitTable_func_column(c *Table_func_columnContext)

	// ExitTable_func_column_list is called when exiting the table_func_column_list production.
	ExitTable_func_column_list(c *Table_func_column_listContext)

	// ExitAlterfunctionstmt is called when exiting the alterfunctionstmt production.
	ExitAlterfunctionstmt(c *AlterfunctionstmtContext)

	// ExitAlterfunc_opt_list is called when exiting the alterfunc_opt_list production.
	ExitAlterfunc_opt_list(c *Alterfunc_opt_listContext)

	// ExitRestrict_ is called when exiting the restrict_ production.
	ExitRestrict_(c *Restrict_Context)

	// ExitRemovefuncstmt is called when exiting the removefuncstmt production.
	ExitRemovefuncstmt(c *RemovefuncstmtContext)

	// ExitRemoveaggrstmt is called when exiting the removeaggrstmt production.
	ExitRemoveaggrstmt(c *RemoveaggrstmtContext)

	// ExitRemoveoperstmt is called when exiting the removeoperstmt production.
	ExitRemoveoperstmt(c *RemoveoperstmtContext)

	// ExitOper_argtypes is called when exiting the oper_argtypes production.
	ExitOper_argtypes(c *Oper_argtypesContext)

	// ExitAny_operator is called when exiting the any_operator production.
	ExitAny_operator(c *Any_operatorContext)

	// ExitOperator_with_argtypes_list is called when exiting the operator_with_argtypes_list production.
	ExitOperator_with_argtypes_list(c *Operator_with_argtypes_listContext)

	// ExitOperator_with_argtypes is called when exiting the operator_with_argtypes production.
	ExitOperator_with_argtypes(c *Operator_with_argtypesContext)

	// ExitDostmt is called when exiting the dostmt production.
	ExitDostmt(c *DostmtContext)

	// ExitDostmt_opt_list is called when exiting the dostmt_opt_list production.
	ExitDostmt_opt_list(c *Dostmt_opt_listContext)

	// ExitDostmt_opt_item is called when exiting the dostmt_opt_item production.
	ExitDostmt_opt_item(c *Dostmt_opt_itemContext)

	// ExitCreatecaststmt is called when exiting the createcaststmt production.
	ExitCreatecaststmt(c *CreatecaststmtContext)

	// ExitCast_context is called when exiting the cast_context production.
	ExitCast_context(c *Cast_contextContext)

	// ExitDropcaststmt is called when exiting the dropcaststmt production.
	ExitDropcaststmt(c *DropcaststmtContext)

	// ExitIf_exists_ is called when exiting the if_exists_ production.
	ExitIf_exists_(c *If_exists_Context)

	// ExitCreatetransformstmt is called when exiting the createtransformstmt production.
	ExitCreatetransformstmt(c *CreatetransformstmtContext)

	// ExitTransform_element_list is called when exiting the transform_element_list production.
	ExitTransform_element_list(c *Transform_element_listContext)

	// ExitDroptransformstmt is called when exiting the droptransformstmt production.
	ExitDroptransformstmt(c *DroptransformstmtContext)

	// ExitReindexstmt is called when exiting the reindexstmt production.
	ExitReindexstmt(c *ReindexstmtContext)

	// ExitReindex_target_relation is called when exiting the reindex_target_relation production.
	ExitReindex_target_relation(c *Reindex_target_relationContext)

	// ExitReindex_target_all is called when exiting the reindex_target_all production.
	ExitReindex_target_all(c *Reindex_target_allContext)

	// ExitReindex_option_list is called when exiting the reindex_option_list production.
	ExitReindex_option_list(c *Reindex_option_listContext)

	// ExitAltertblspcstmt is called when exiting the altertblspcstmt production.
	ExitAltertblspcstmt(c *AltertblspcstmtContext)

	// ExitRenamestmt is called when exiting the renamestmt production.
	ExitRenamestmt(c *RenamestmtContext)

	// ExitColumn_ is called when exiting the column_ production.
	ExitColumn_(c *Column_Context)

	// ExitSet_data_ is called when exiting the set_data_ production.
	ExitSet_data_(c *Set_data_Context)

	// ExitAlterobjectdependsstmt is called when exiting the alterobjectdependsstmt production.
	ExitAlterobjectdependsstmt(c *AlterobjectdependsstmtContext)

	// ExitNo_ is called when exiting the no_ production.
	ExitNo_(c *No_Context)

	// ExitAlterobjectschemastmt is called when exiting the alterobjectschemastmt production.
	ExitAlterobjectschemastmt(c *AlterobjectschemastmtContext)

	// ExitAlteroperatorstmt is called when exiting the alteroperatorstmt production.
	ExitAlteroperatorstmt(c *AlteroperatorstmtContext)

	// ExitOperator_def_list is called when exiting the operator_def_list production.
	ExitOperator_def_list(c *Operator_def_listContext)

	// ExitOperator_def_elem is called when exiting the operator_def_elem production.
	ExitOperator_def_elem(c *Operator_def_elemContext)

	// ExitOperator_def_arg is called when exiting the operator_def_arg production.
	ExitOperator_def_arg(c *Operator_def_argContext)

	// ExitAltertypestmt is called when exiting the altertypestmt production.
	ExitAltertypestmt(c *AltertypestmtContext)

	// ExitAlterownerstmt is called when exiting the alterownerstmt production.
	ExitAlterownerstmt(c *AlterownerstmtContext)

	// ExitCreatepublicationstmt is called when exiting the createpublicationstmt production.
	ExitCreatepublicationstmt(c *CreatepublicationstmtContext)

	// ExitPublication_for_tables_ is called when exiting the publication_for_tables_ production.
	ExitPublication_for_tables_(c *Publication_for_tables_Context)

	// ExitPublication_for_tables is called when exiting the publication_for_tables production.
	ExitPublication_for_tables(c *Publication_for_tablesContext)

	// ExitAlterpublicationstmt is called when exiting the alterpublicationstmt production.
	ExitAlterpublicationstmt(c *AlterpublicationstmtContext)

	// ExitCreatesubscriptionstmt is called when exiting the createsubscriptionstmt production.
	ExitCreatesubscriptionstmt(c *CreatesubscriptionstmtContext)

	// ExitPublication_name_list is called when exiting the publication_name_list production.
	ExitPublication_name_list(c *Publication_name_listContext)

	// ExitPublication_name_item is called when exiting the publication_name_item production.
	ExitPublication_name_item(c *Publication_name_itemContext)

	// ExitAltersubscriptionstmt is called when exiting the altersubscriptionstmt production.
	ExitAltersubscriptionstmt(c *AltersubscriptionstmtContext)

	// ExitDropsubscriptionstmt is called when exiting the dropsubscriptionstmt production.
	ExitDropsubscriptionstmt(c *DropsubscriptionstmtContext)

	// ExitRulestmt is called when exiting the rulestmt production.
	ExitRulestmt(c *RulestmtContext)

	// ExitRuleactionlist is called when exiting the ruleactionlist production.
	ExitRuleactionlist(c *RuleactionlistContext)

	// ExitRuleactionmulti is called when exiting the ruleactionmulti production.
	ExitRuleactionmulti(c *RuleactionmultiContext)

	// ExitRuleactionstmt is called when exiting the ruleactionstmt production.
	ExitRuleactionstmt(c *RuleactionstmtContext)

	// ExitRuleactionstmtOrEmpty is called when exiting the ruleactionstmtOrEmpty production.
	ExitRuleactionstmtOrEmpty(c *RuleactionstmtOrEmptyContext)

	// ExitEvent is called when exiting the event production.
	ExitEvent(c *EventContext)

	// ExitInstead_ is called when exiting the instead_ production.
	ExitInstead_(c *Instead_Context)

	// ExitNotifystmt is called when exiting the notifystmt production.
	ExitNotifystmt(c *NotifystmtContext)

	// ExitNotify_payload is called when exiting the notify_payload production.
	ExitNotify_payload(c *Notify_payloadContext)

	// ExitListenstmt is called when exiting the listenstmt production.
	ExitListenstmt(c *ListenstmtContext)

	// ExitUnlistenstmt is called when exiting the unlistenstmt production.
	ExitUnlistenstmt(c *UnlistenstmtContext)

	// ExitTransactionstmt is called when exiting the transactionstmt production.
	ExitTransactionstmt(c *TransactionstmtContext)

	// ExitTransaction_ is called when exiting the transaction_ production.
	ExitTransaction_(c *Transaction_Context)

	// ExitTransaction_mode_item is called when exiting the transaction_mode_item production.
	ExitTransaction_mode_item(c *Transaction_mode_itemContext)

	// ExitTransaction_mode_list is called when exiting the transaction_mode_list production.
	ExitTransaction_mode_list(c *Transaction_mode_listContext)

	// ExitTransaction_mode_list_or_empty is called when exiting the transaction_mode_list_or_empty production.
	ExitTransaction_mode_list_or_empty(c *Transaction_mode_list_or_emptyContext)

	// ExitTransaction_chain_ is called when exiting the transaction_chain_ production.
	ExitTransaction_chain_(c *Transaction_chain_Context)

	// ExitViewstmt is called when exiting the viewstmt production.
	ExitViewstmt(c *ViewstmtContext)

	// ExitCheck_option_ is called when exiting the check_option_ production.
	ExitCheck_option_(c *Check_option_Context)

	// ExitLoadstmt is called when exiting the loadstmt production.
	ExitLoadstmt(c *LoadstmtContext)

	// ExitCreatedbstmt is called when exiting the createdbstmt production.
	ExitCreatedbstmt(c *CreatedbstmtContext)

	// ExitCreatedb_opt_list is called when exiting the createdb_opt_list production.
	ExitCreatedb_opt_list(c *Createdb_opt_listContext)

	// ExitCreatedb_opt_items is called when exiting the createdb_opt_items production.
	ExitCreatedb_opt_items(c *Createdb_opt_itemsContext)

	// ExitCreatedb_opt_item is called when exiting the createdb_opt_item production.
	ExitCreatedb_opt_item(c *Createdb_opt_itemContext)

	// ExitCreatedb_opt_name is called when exiting the createdb_opt_name production.
	ExitCreatedb_opt_name(c *Createdb_opt_nameContext)

	// ExitEqual_ is called when exiting the equal_ production.
	ExitEqual_(c *Equal_Context)

	// ExitAlterdatabasestmt is called when exiting the alterdatabasestmt production.
	ExitAlterdatabasestmt(c *AlterdatabasestmtContext)

	// ExitAlterdatabasesetstmt is called when exiting the alterdatabasesetstmt production.
	ExitAlterdatabasesetstmt(c *AlterdatabasesetstmtContext)

	// ExitDropdbstmt is called when exiting the dropdbstmt production.
	ExitDropdbstmt(c *DropdbstmtContext)

	// ExitDrop_option_list is called when exiting the drop_option_list production.
	ExitDrop_option_list(c *Drop_option_listContext)

	// ExitDrop_option is called when exiting the drop_option production.
	ExitDrop_option(c *Drop_optionContext)

	// ExitAltercollationstmt is called when exiting the altercollationstmt production.
	ExitAltercollationstmt(c *AltercollationstmtContext)

	// ExitAltersystemstmt is called when exiting the altersystemstmt production.
	ExitAltersystemstmt(c *AltersystemstmtContext)

	// ExitCreatedomainstmt is called when exiting the createdomainstmt production.
	ExitCreatedomainstmt(c *CreatedomainstmtContext)

	// ExitAlterdomainstmt is called when exiting the alterdomainstmt production.
	ExitAlterdomainstmt(c *AlterdomainstmtContext)

	// ExitAs_ is called when exiting the as_ production.
	ExitAs_(c *As_Context)

	// ExitAltertsdictionarystmt is called when exiting the altertsdictionarystmt production.
	ExitAltertsdictionarystmt(c *AltertsdictionarystmtContext)

	// ExitAltertsconfigurationstmt is called when exiting the altertsconfigurationstmt production.
	ExitAltertsconfigurationstmt(c *AltertsconfigurationstmtContext)

	// ExitAny_with is called when exiting the any_with production.
	ExitAny_with(c *Any_withContext)

	// ExitCreateconversionstmt is called when exiting the createconversionstmt production.
	ExitCreateconversionstmt(c *CreateconversionstmtContext)

	// ExitClusterstmt is called when exiting the clusterstmt production.
	ExitClusterstmt(c *ClusterstmtContext)

	// ExitCluster_index_specification is called when exiting the cluster_index_specification production.
	ExitCluster_index_specification(c *Cluster_index_specificationContext)

	// ExitVacuumstmt is called when exiting the vacuumstmt production.
	ExitVacuumstmt(c *VacuumstmtContext)

	// ExitAnalyzestmt is called when exiting the analyzestmt production.
	ExitAnalyzestmt(c *AnalyzestmtContext)

	// ExitUtility_option_list is called when exiting the utility_option_list production.
	ExitUtility_option_list(c *Utility_option_listContext)

	// ExitVac_analyze_option_list is called when exiting the vac_analyze_option_list production.
	ExitVac_analyze_option_list(c *Vac_analyze_option_listContext)

	// ExitAnalyze_keyword is called when exiting the analyze_keyword production.
	ExitAnalyze_keyword(c *Analyze_keywordContext)

	// ExitUtility_option_elem is called when exiting the utility_option_elem production.
	ExitUtility_option_elem(c *Utility_option_elemContext)

	// ExitUtility_option_name is called when exiting the utility_option_name production.
	ExitUtility_option_name(c *Utility_option_nameContext)

	// ExitUtility_option_arg is called when exiting the utility_option_arg production.
	ExitUtility_option_arg(c *Utility_option_argContext)

	// ExitVac_analyze_option_elem is called when exiting the vac_analyze_option_elem production.
	ExitVac_analyze_option_elem(c *Vac_analyze_option_elemContext)

	// ExitVac_analyze_option_name is called when exiting the vac_analyze_option_name production.
	ExitVac_analyze_option_name(c *Vac_analyze_option_nameContext)

	// ExitVac_analyze_option_arg is called when exiting the vac_analyze_option_arg production.
	ExitVac_analyze_option_arg(c *Vac_analyze_option_argContext)

	// ExitAnalyze_ is called when exiting the analyze_ production.
	ExitAnalyze_(c *Analyze_Context)

	// ExitVerbose_ is called when exiting the verbose_ production.
	ExitVerbose_(c *Verbose_Context)

	// ExitFull_ is called when exiting the full_ production.
	ExitFull_(c *Full_Context)

	// ExitFreeze_ is called when exiting the freeze_ production.
	ExitFreeze_(c *Freeze_Context)

	// ExitName_list_ is called when exiting the name_list_ production.
	ExitName_list_(c *Name_list_Context)

	// ExitVacuum_relation is called when exiting the vacuum_relation production.
	ExitVacuum_relation(c *Vacuum_relationContext)

	// ExitVacuum_relation_list is called when exiting the vacuum_relation_list production.
	ExitVacuum_relation_list(c *Vacuum_relation_listContext)

	// ExitVacuum_relation_list_ is called when exiting the vacuum_relation_list_ production.
	ExitVacuum_relation_list_(c *Vacuum_relation_list_Context)

	// ExitExplainstmt is called when exiting the explainstmt production.
	ExitExplainstmt(c *ExplainstmtContext)

	// ExitExplainablestmt is called when exiting the explainablestmt production.
	ExitExplainablestmt(c *ExplainablestmtContext)

	// ExitExplain_option_list is called when exiting the explain_option_list production.
	ExitExplain_option_list(c *Explain_option_listContext)

	// ExitExplain_option_elem is called when exiting the explain_option_elem production.
	ExitExplain_option_elem(c *Explain_option_elemContext)

	// ExitExplain_option_name is called when exiting the explain_option_name production.
	ExitExplain_option_name(c *Explain_option_nameContext)

	// ExitExplain_option_arg is called when exiting the explain_option_arg production.
	ExitExplain_option_arg(c *Explain_option_argContext)

	// ExitPreparestmt is called when exiting the preparestmt production.
	ExitPreparestmt(c *PreparestmtContext)

	// ExitPrep_type_clause is called when exiting the prep_type_clause production.
	ExitPrep_type_clause(c *Prep_type_clauseContext)

	// ExitPreparablestmt is called when exiting the preparablestmt production.
	ExitPreparablestmt(c *PreparablestmtContext)

	// ExitExecutestmt is called when exiting the executestmt production.
	ExitExecutestmt(c *ExecutestmtContext)

	// ExitExecute_param_clause is called when exiting the execute_param_clause production.
	ExitExecute_param_clause(c *Execute_param_clauseContext)

	// ExitDeallocatestmt is called when exiting the deallocatestmt production.
	ExitDeallocatestmt(c *DeallocatestmtContext)

	// ExitInsertstmt is called when exiting the insertstmt production.
	ExitInsertstmt(c *InsertstmtContext)

	// ExitInsert_target is called when exiting the insert_target production.
	ExitInsert_target(c *Insert_targetContext)

	// ExitInsert_rest is called when exiting the insert_rest production.
	ExitInsert_rest(c *Insert_restContext)

	// ExitOverride_kind is called when exiting the override_kind production.
	ExitOverride_kind(c *Override_kindContext)

	// ExitInsert_column_list is called when exiting the insert_column_list production.
	ExitInsert_column_list(c *Insert_column_listContext)

	// ExitInsert_column_item is called when exiting the insert_column_item production.
	ExitInsert_column_item(c *Insert_column_itemContext)

	// ExitOn_conflict_ is called when exiting the on_conflict_ production.
	ExitOn_conflict_(c *On_conflict_Context)

	// ExitConf_expr_ is called when exiting the conf_expr_ production.
	ExitConf_expr_(c *Conf_expr_Context)

	// ExitReturning_clause is called when exiting the returning_clause production.
	ExitReturning_clause(c *Returning_clauseContext)

	// ExitMergestmt is called when exiting the mergestmt production.
	ExitMergestmt(c *MergestmtContext)

	// ExitMerge_insert_clause is called when exiting the merge_insert_clause production.
	ExitMerge_insert_clause(c *Merge_insert_clauseContext)

	// ExitMerge_update_clause is called when exiting the merge_update_clause production.
	ExitMerge_update_clause(c *Merge_update_clauseContext)

	// ExitMerge_delete_clause is called when exiting the merge_delete_clause production.
	ExitMerge_delete_clause(c *Merge_delete_clauseContext)

	// ExitDeletestmt is called when exiting the deletestmt production.
	ExitDeletestmt(c *DeletestmtContext)

	// ExitUsing_clause is called when exiting the using_clause production.
	ExitUsing_clause(c *Using_clauseContext)

	// ExitLockstmt is called when exiting the lockstmt production.
	ExitLockstmt(c *LockstmtContext)

	// ExitLock_ is called when exiting the lock_ production.
	ExitLock_(c *Lock_Context)

	// ExitLock_type is called when exiting the lock_type production.
	ExitLock_type(c *Lock_typeContext)

	// ExitNowait_ is called when exiting the nowait_ production.
	ExitNowait_(c *Nowait_Context)

	// ExitNowait_or_skip_ is called when exiting the nowait_or_skip_ production.
	ExitNowait_or_skip_(c *Nowait_or_skip_Context)

	// ExitUpdatestmt is called when exiting the updatestmt production.
	ExitUpdatestmt(c *UpdatestmtContext)

	// ExitSet_clause_list is called when exiting the set_clause_list production.
	ExitSet_clause_list(c *Set_clause_listContext)

	// ExitSet_clause is called when exiting the set_clause production.
	ExitSet_clause(c *Set_clauseContext)

	// ExitSet_target is called when exiting the set_target production.
	ExitSet_target(c *Set_targetContext)

	// ExitSet_target_list is called when exiting the set_target_list production.
	ExitSet_target_list(c *Set_target_listContext)

	// ExitDeclarecursorstmt is called when exiting the declarecursorstmt production.
	ExitDeclarecursorstmt(c *DeclarecursorstmtContext)

	// ExitCursor_name is called when exiting the cursor_name production.
	ExitCursor_name(c *Cursor_nameContext)

	// ExitCursor_options is called when exiting the cursor_options production.
	ExitCursor_options(c *Cursor_optionsContext)

	// ExitHold_ is called when exiting the hold_ production.
	ExitHold_(c *Hold_Context)

	// ExitSelectstmt is called when exiting the selectstmt production.
	ExitSelectstmt(c *SelectstmtContext)

	// ExitSelect_with_parens is called when exiting the select_with_parens production.
	ExitSelect_with_parens(c *Select_with_parensContext)

	// ExitSelect_no_parens is called when exiting the select_no_parens production.
	ExitSelect_no_parens(c *Select_no_parensContext)

	// ExitSelect_clause is called when exiting the select_clause production.
	ExitSelect_clause(c *Select_clauseContext)

	// ExitSimple_select_intersect is called when exiting the simple_select_intersect production.
	ExitSimple_select_intersect(c *Simple_select_intersectContext)

	// ExitSimple_select_pramary is called when exiting the simple_select_pramary production.
	ExitSimple_select_pramary(c *Simple_select_pramaryContext)

	// ExitWith_clause is called when exiting the with_clause production.
	ExitWith_clause(c *With_clauseContext)

	// ExitCte_list is called when exiting the cte_list production.
	ExitCte_list(c *Cte_listContext)

	// ExitCommon_table_expr is called when exiting the common_table_expr production.
	ExitCommon_table_expr(c *Common_table_exprContext)

	// ExitMaterialized_ is called when exiting the materialized_ production.
	ExitMaterialized_(c *Materialized_Context)

	// ExitWith_clause_ is called when exiting the with_clause_ production.
	ExitWith_clause_(c *With_clause_Context)

	// ExitInto_clause is called when exiting the into_clause production.
	ExitInto_clause(c *Into_clauseContext)

	// ExitStrict_ is called when exiting the strict_ production.
	ExitStrict_(c *Strict_Context)

	// ExitOpttempTableName is called when exiting the opttempTableName production.
	ExitOpttempTableName(c *OpttempTableNameContext)

	// ExitTable_ is called when exiting the table_ production.
	ExitTable_(c *Table_Context)

	// ExitAll_or_distinct is called when exiting the all_or_distinct production.
	ExitAll_or_distinct(c *All_or_distinctContext)

	// ExitDistinct_clause is called when exiting the distinct_clause production.
	ExitDistinct_clause(c *Distinct_clauseContext)

	// ExitAll_clause_ is called when exiting the all_clause_ production.
	ExitAll_clause_(c *All_clause_Context)

	// ExitSort_clause_ is called when exiting the sort_clause_ production.
	ExitSort_clause_(c *Sort_clause_Context)

	// ExitSort_clause is called when exiting the sort_clause production.
	ExitSort_clause(c *Sort_clauseContext)

	// ExitSortby_list is called when exiting the sortby_list production.
	ExitSortby_list(c *Sortby_listContext)

	// ExitSortby is called when exiting the sortby production.
	ExitSortby(c *SortbyContext)

	// ExitSelect_limit is called when exiting the select_limit production.
	ExitSelect_limit(c *Select_limitContext)

	// ExitSelect_limit_ is called when exiting the select_limit_ production.
	ExitSelect_limit_(c *Select_limit_Context)

	// ExitLimit_clause is called when exiting the limit_clause production.
	ExitLimit_clause(c *Limit_clauseContext)

	// ExitOffset_clause is called when exiting the offset_clause production.
	ExitOffset_clause(c *Offset_clauseContext)

	// ExitSelect_limit_value is called when exiting the select_limit_value production.
	ExitSelect_limit_value(c *Select_limit_valueContext)

	// ExitSelect_offset_value is called when exiting the select_offset_value production.
	ExitSelect_offset_value(c *Select_offset_valueContext)

	// ExitSelect_fetch_first_value is called when exiting the select_fetch_first_value production.
	ExitSelect_fetch_first_value(c *Select_fetch_first_valueContext)

	// ExitI_or_f_const is called when exiting the i_or_f_const production.
	ExitI_or_f_const(c *I_or_f_constContext)

	// ExitRow_or_rows is called when exiting the row_or_rows production.
	ExitRow_or_rows(c *Row_or_rowsContext)

	// ExitFirst_or_next is called when exiting the first_or_next production.
	ExitFirst_or_next(c *First_or_nextContext)

	// ExitGroup_clause is called when exiting the group_clause production.
	ExitGroup_clause(c *Group_clauseContext)

	// ExitGroup_by_list is called when exiting the group_by_list production.
	ExitGroup_by_list(c *Group_by_listContext)

	// ExitGroup_by_item is called when exiting the group_by_item production.
	ExitGroup_by_item(c *Group_by_itemContext)

	// ExitEmpty_grouping_set is called when exiting the empty_grouping_set production.
	ExitEmpty_grouping_set(c *Empty_grouping_setContext)

	// ExitRollup_clause is called when exiting the rollup_clause production.
	ExitRollup_clause(c *Rollup_clauseContext)

	// ExitCube_clause is called when exiting the cube_clause production.
	ExitCube_clause(c *Cube_clauseContext)

	// ExitGrouping_sets_clause is called when exiting the grouping_sets_clause production.
	ExitGrouping_sets_clause(c *Grouping_sets_clauseContext)

	// ExitHaving_clause is called when exiting the having_clause production.
	ExitHaving_clause(c *Having_clauseContext)

	// ExitFor_locking_clause is called when exiting the for_locking_clause production.
	ExitFor_locking_clause(c *For_locking_clauseContext)

	// ExitFor_locking_clause_ is called when exiting the for_locking_clause_ production.
	ExitFor_locking_clause_(c *For_locking_clause_Context)

	// ExitFor_locking_items is called when exiting the for_locking_items production.
	ExitFor_locking_items(c *For_locking_itemsContext)

	// ExitFor_locking_item is called when exiting the for_locking_item production.
	ExitFor_locking_item(c *For_locking_itemContext)

	// ExitFor_locking_strength is called when exiting the for_locking_strength production.
	ExitFor_locking_strength(c *For_locking_strengthContext)

	// ExitLocked_rels_list is called when exiting the locked_rels_list production.
	ExitLocked_rels_list(c *Locked_rels_listContext)

	// ExitValues_clause is called when exiting the values_clause production.
	ExitValues_clause(c *Values_clauseContext)

	// ExitFrom_clause is called when exiting the from_clause production.
	ExitFrom_clause(c *From_clauseContext)

	// ExitFrom_list is called when exiting the from_list production.
	ExitFrom_list(c *From_listContext)

	// ExitTable_ref is called when exiting the table_ref production.
	ExitTable_ref(c *Table_refContext)

	// ExitAlias_clause is called when exiting the alias_clause production.
	ExitAlias_clause(c *Alias_clauseContext)

	// ExitFunc_alias_clause is called when exiting the func_alias_clause production.
	ExitFunc_alias_clause(c *Func_alias_clauseContext)

	// ExitJoin_type is called when exiting the join_type production.
	ExitJoin_type(c *Join_typeContext)

	// ExitJoin_qual is called when exiting the join_qual production.
	ExitJoin_qual(c *Join_qualContext)

	// ExitRelation_expr is called when exiting the relation_expr production.
	ExitRelation_expr(c *Relation_exprContext)

	// ExitRelation_expr_list is called when exiting the relation_expr_list production.
	ExitRelation_expr_list(c *Relation_expr_listContext)

	// ExitRelation_expr_opt_alias is called when exiting the relation_expr_opt_alias production.
	ExitRelation_expr_opt_alias(c *Relation_expr_opt_aliasContext)

	// ExitTablesample_clause is called when exiting the tablesample_clause production.
	ExitTablesample_clause(c *Tablesample_clauseContext)

	// ExitRepeatable_clause_ is called when exiting the repeatable_clause_ production.
	ExitRepeatable_clause_(c *Repeatable_clause_Context)

	// ExitFunc_table is called when exiting the func_table production.
	ExitFunc_table(c *Func_tableContext)

	// ExitRowsfrom_item is called when exiting the rowsfrom_item production.
	ExitRowsfrom_item(c *Rowsfrom_itemContext)

	// ExitRowsfrom_list is called when exiting the rowsfrom_list production.
	ExitRowsfrom_list(c *Rowsfrom_listContext)

	// ExitCol_def_list_ is called when exiting the col_def_list_ production.
	ExitCol_def_list_(c *Col_def_list_Context)

	// ExitOrdinality_ is called when exiting the ordinality_ production.
	ExitOrdinality_(c *Ordinality_Context)

	// ExitWhere_clause is called when exiting the where_clause production.
	ExitWhere_clause(c *Where_clauseContext)

	// ExitWhere_or_current_clause is called when exiting the where_or_current_clause production.
	ExitWhere_or_current_clause(c *Where_or_current_clauseContext)

	// ExitOpttablefuncelementlist is called when exiting the opttablefuncelementlist production.
	ExitOpttablefuncelementlist(c *OpttablefuncelementlistContext)

	// ExitTablefuncelementlist is called when exiting the tablefuncelementlist production.
	ExitTablefuncelementlist(c *TablefuncelementlistContext)

	// ExitTablefuncelement is called when exiting the tablefuncelement production.
	ExitTablefuncelement(c *TablefuncelementContext)

	// ExitXmltable is called when exiting the xmltable production.
	ExitXmltable(c *XmltableContext)

	// ExitXmltable_column_list is called when exiting the xmltable_column_list production.
	ExitXmltable_column_list(c *Xmltable_column_listContext)

	// ExitXmltable_column_el is called when exiting the xmltable_column_el production.
	ExitXmltable_column_el(c *Xmltable_column_elContext)

	// ExitXmltable_column_option_list is called when exiting the xmltable_column_option_list production.
	ExitXmltable_column_option_list(c *Xmltable_column_option_listContext)

	// ExitXmltable_column_option_el is called when exiting the xmltable_column_option_el production.
	ExitXmltable_column_option_el(c *Xmltable_column_option_elContext)

	// ExitXml_namespace_list is called when exiting the xml_namespace_list production.
	ExitXml_namespace_list(c *Xml_namespace_listContext)

	// ExitXml_namespace_el is called when exiting the xml_namespace_el production.
	ExitXml_namespace_el(c *Xml_namespace_elContext)

	// ExitTypename is called when exiting the typename production.
	ExitTypename(c *TypenameContext)

	// ExitOpt_array_bounds is called when exiting the opt_array_bounds production.
	ExitOpt_array_bounds(c *Opt_array_boundsContext)

	// ExitSimpletypename is called when exiting the simpletypename production.
	ExitSimpletypename(c *SimpletypenameContext)

	// ExitConsttypename is called when exiting the consttypename production.
	ExitConsttypename(c *ConsttypenameContext)

	// ExitGenerictype is called when exiting the generictype production.
	ExitGenerictype(c *GenerictypeContext)

	// ExitType_modifiers_ is called when exiting the type_modifiers_ production.
	ExitType_modifiers_(c *Type_modifiers_Context)

	// ExitNumeric is called when exiting the numeric production.
	ExitNumeric(c *NumericContext)

	// ExitFloat_ is called when exiting the float_ production.
	ExitFloat_(c *Float_Context)

	// ExitBit is called when exiting the bit production.
	ExitBit(c *BitContext)

	// ExitConstbit is called when exiting the constbit production.
	ExitConstbit(c *ConstbitContext)

	// ExitBitwithlength is called when exiting the bitwithlength production.
	ExitBitwithlength(c *BitwithlengthContext)

	// ExitBitwithoutlength is called when exiting the bitwithoutlength production.
	ExitBitwithoutlength(c *BitwithoutlengthContext)

	// ExitCharacter is called when exiting the character production.
	ExitCharacter(c *CharacterContext)

	// ExitConstcharacter is called when exiting the constcharacter production.
	ExitConstcharacter(c *ConstcharacterContext)

	// ExitCharacter_c is called when exiting the character_c production.
	ExitCharacter_c(c *Character_cContext)

	// ExitVarying_ is called when exiting the varying_ production.
	ExitVarying_(c *Varying_Context)

	// ExitConstdatetime is called when exiting the constdatetime production.
	ExitConstdatetime(c *ConstdatetimeContext)

	// ExitConstinterval is called when exiting the constinterval production.
	ExitConstinterval(c *ConstintervalContext)

	// ExitTimezone_ is called when exiting the timezone_ production.
	ExitTimezone_(c *Timezone_Context)

	// ExitInterval_ is called when exiting the interval_ production.
	ExitInterval_(c *Interval_Context)

	// ExitInterval_second is called when exiting the interval_second production.
	ExitInterval_second(c *Interval_secondContext)

	// ExitJsonType is called when exiting the jsonType production.
	ExitJsonType(c *JsonTypeContext)

	// ExitEscape_ is called when exiting the escape_ production.
	ExitEscape_(c *Escape_Context)

	// ExitA_expr is called when exiting the a_expr production.
	ExitA_expr(c *A_exprContext)

	// ExitA_expr_qual is called when exiting the a_expr_qual production.
	ExitA_expr_qual(c *A_expr_qualContext)

	// ExitA_expr_lessless is called when exiting the a_expr_lessless production.
	ExitA_expr_lessless(c *A_expr_lesslessContext)

	// ExitA_expr_or is called when exiting the a_expr_or production.
	ExitA_expr_or(c *A_expr_orContext)

	// ExitA_expr_and is called when exiting the a_expr_and production.
	ExitA_expr_and(c *A_expr_andContext)

	// ExitA_expr_between is called when exiting the a_expr_between production.
	ExitA_expr_between(c *A_expr_betweenContext)

	// ExitA_expr_in is called when exiting the a_expr_in production.
	ExitA_expr_in(c *A_expr_inContext)

	// ExitA_expr_unary_not is called when exiting the a_expr_unary_not production.
	ExitA_expr_unary_not(c *A_expr_unary_notContext)

	// ExitA_expr_isnull is called when exiting the a_expr_isnull production.
	ExitA_expr_isnull(c *A_expr_isnullContext)

	// ExitA_expr_is_not is called when exiting the a_expr_is_not production.
	ExitA_expr_is_not(c *A_expr_is_notContext)

	// ExitA_expr_compare is called when exiting the a_expr_compare production.
	ExitA_expr_compare(c *A_expr_compareContext)

	// ExitA_expr_like is called when exiting the a_expr_like production.
	ExitA_expr_like(c *A_expr_likeContext)

	// ExitA_expr_qual_op is called when exiting the a_expr_qual_op production.
	ExitA_expr_qual_op(c *A_expr_qual_opContext)

	// ExitA_expr_unary_qualop is called when exiting the a_expr_unary_qualop production.
	ExitA_expr_unary_qualop(c *A_expr_unary_qualopContext)

	// ExitA_expr_add is called when exiting the a_expr_add production.
	ExitA_expr_add(c *A_expr_addContext)

	// ExitA_expr_mul is called when exiting the a_expr_mul production.
	ExitA_expr_mul(c *A_expr_mulContext)

	// ExitA_expr_caret is called when exiting the a_expr_caret production.
	ExitA_expr_caret(c *A_expr_caretContext)

	// ExitA_expr_unary_sign is called when exiting the a_expr_unary_sign production.
	ExitA_expr_unary_sign(c *A_expr_unary_signContext)

	// ExitA_expr_at_time_zone is called when exiting the a_expr_at_time_zone production.
	ExitA_expr_at_time_zone(c *A_expr_at_time_zoneContext)

	// ExitA_expr_collate is called when exiting the a_expr_collate production.
	ExitA_expr_collate(c *A_expr_collateContext)

	// ExitA_expr_typecast is called when exiting the a_expr_typecast production.
	ExitA_expr_typecast(c *A_expr_typecastContext)

	// ExitB_expr is called when exiting the b_expr production.
	ExitB_expr(c *B_exprContext)

	// ExitC_expr_exists is called when exiting the c_expr_exists production.
	ExitC_expr_exists(c *C_expr_existsContext)

	// ExitC_expr_expr is called when exiting the c_expr_expr production.
	ExitC_expr_expr(c *C_expr_exprContext)

	// ExitC_expr_case is called when exiting the c_expr_case production.
	ExitC_expr_case(c *C_expr_caseContext)

	// ExitPlsqlvariablename is called when exiting the plsqlvariablename production.
	ExitPlsqlvariablename(c *PlsqlvariablenameContext)

	// ExitFunc_application is called when exiting the func_application production.
	ExitFunc_application(c *Func_applicationContext)

	// ExitFunc_expr is called when exiting the func_expr production.
	ExitFunc_expr(c *Func_exprContext)

	// ExitFunc_expr_windowless is called when exiting the func_expr_windowless production.
	ExitFunc_expr_windowless(c *Func_expr_windowlessContext)

	// ExitFunc_expr_common_subexpr is called when exiting the func_expr_common_subexpr production.
	ExitFunc_expr_common_subexpr(c *Func_expr_common_subexprContext)

	// ExitXml_root_version is called when exiting the xml_root_version production.
	ExitXml_root_version(c *Xml_root_versionContext)

	// ExitXml_root_standalone_ is called when exiting the xml_root_standalone_ production.
	ExitXml_root_standalone_(c *Xml_root_standalone_Context)

	// ExitXml_attributes is called when exiting the xml_attributes production.
	ExitXml_attributes(c *Xml_attributesContext)

	// ExitXml_attribute_list is called when exiting the xml_attribute_list production.
	ExitXml_attribute_list(c *Xml_attribute_listContext)

	// ExitXml_attribute_el is called when exiting the xml_attribute_el production.
	ExitXml_attribute_el(c *Xml_attribute_elContext)

	// ExitDocument_or_content is called when exiting the document_or_content production.
	ExitDocument_or_content(c *Document_or_contentContext)

	// ExitXml_whitespace_option is called when exiting the xml_whitespace_option production.
	ExitXml_whitespace_option(c *Xml_whitespace_optionContext)

	// ExitXmlexists_argument is called when exiting the xmlexists_argument production.
	ExitXmlexists_argument(c *Xmlexists_argumentContext)

	// ExitXml_passing_mech is called when exiting the xml_passing_mech production.
	ExitXml_passing_mech(c *Xml_passing_mechContext)

	// ExitWithin_group_clause is called when exiting the within_group_clause production.
	ExitWithin_group_clause(c *Within_group_clauseContext)

	// ExitFilter_clause is called when exiting the filter_clause production.
	ExitFilter_clause(c *Filter_clauseContext)

	// ExitWindow_clause is called when exiting the window_clause production.
	ExitWindow_clause(c *Window_clauseContext)

	// ExitWindow_definition_list is called when exiting the window_definition_list production.
	ExitWindow_definition_list(c *Window_definition_listContext)

	// ExitWindow_definition is called when exiting the window_definition production.
	ExitWindow_definition(c *Window_definitionContext)

	// ExitOver_clause is called when exiting the over_clause production.
	ExitOver_clause(c *Over_clauseContext)

	// ExitWindow_specification is called when exiting the window_specification production.
	ExitWindow_specification(c *Window_specificationContext)

	// ExitExisting_window_name_ is called when exiting the existing_window_name_ production.
	ExitExisting_window_name_(c *Existing_window_name_Context)

	// ExitPartition_clause_ is called when exiting the partition_clause_ production.
	ExitPartition_clause_(c *Partition_clause_Context)

	// ExitFrame_clause_ is called when exiting the frame_clause_ production.
	ExitFrame_clause_(c *Frame_clause_Context)

	// ExitFrame_extent is called when exiting the frame_extent production.
	ExitFrame_extent(c *Frame_extentContext)

	// ExitFrame_bound is called when exiting the frame_bound production.
	ExitFrame_bound(c *Frame_boundContext)

	// ExitWindow_exclusion_clause_ is called when exiting the window_exclusion_clause_ production.
	ExitWindow_exclusion_clause_(c *Window_exclusion_clause_Context)

	// ExitRow is called when exiting the row production.
	ExitRow(c *RowContext)

	// ExitExplicit_row is called when exiting the explicit_row production.
	ExitExplicit_row(c *Explicit_rowContext)

	// ExitImplicit_row is called when exiting the implicit_row production.
	ExitImplicit_row(c *Implicit_rowContext)

	// ExitSub_type is called when exiting the sub_type production.
	ExitSub_type(c *Sub_typeContext)

	// ExitAll_op is called when exiting the all_op production.
	ExitAll_op(c *All_opContext)

	// ExitMathop is called when exiting the mathop production.
	ExitMathop(c *MathopContext)

	// ExitQual_op is called when exiting the qual_op production.
	ExitQual_op(c *Qual_opContext)

	// ExitQual_all_op is called when exiting the qual_all_op production.
	ExitQual_all_op(c *Qual_all_opContext)

	// ExitSubquery_Op is called when exiting the subquery_Op production.
	ExitSubquery_Op(c *Subquery_OpContext)

	// ExitExpr_list is called when exiting the expr_list production.
	ExitExpr_list(c *Expr_listContext)

	// ExitFunc_arg_list is called when exiting the func_arg_list production.
	ExitFunc_arg_list(c *Func_arg_listContext)

	// ExitFunc_arg_expr is called when exiting the func_arg_expr production.
	ExitFunc_arg_expr(c *Func_arg_exprContext)

	// ExitType_list is called when exiting the type_list production.
	ExitType_list(c *Type_listContext)

	// ExitArray_expr is called when exiting the array_expr production.
	ExitArray_expr(c *Array_exprContext)

	// ExitArray_expr_list is called when exiting the array_expr_list production.
	ExitArray_expr_list(c *Array_expr_listContext)

	// ExitExtract_list is called when exiting the extract_list production.
	ExitExtract_list(c *Extract_listContext)

	// ExitExtract_arg is called when exiting the extract_arg production.
	ExitExtract_arg(c *Extract_argContext)

	// ExitUnicode_normal_form is called when exiting the unicode_normal_form production.
	ExitUnicode_normal_form(c *Unicode_normal_formContext)

	// ExitOverlay_list is called when exiting the overlay_list production.
	ExitOverlay_list(c *Overlay_listContext)

	// ExitPosition_list is called when exiting the position_list production.
	ExitPosition_list(c *Position_listContext)

	// ExitSubstr_list is called when exiting the substr_list production.
	ExitSubstr_list(c *Substr_listContext)

	// ExitTrim_list is called when exiting the trim_list production.
	ExitTrim_list(c *Trim_listContext)

	// ExitIn_expr_select is called when exiting the in_expr_select production.
	ExitIn_expr_select(c *In_expr_selectContext)

	// ExitIn_expr_list is called when exiting the in_expr_list production.
	ExitIn_expr_list(c *In_expr_listContext)

	// ExitCase_expr is called when exiting the case_expr production.
	ExitCase_expr(c *Case_exprContext)

	// ExitWhen_clause_list is called when exiting the when_clause_list production.
	ExitWhen_clause_list(c *When_clause_listContext)

	// ExitWhen_clause is called when exiting the when_clause production.
	ExitWhen_clause(c *When_clauseContext)

	// ExitCase_default is called when exiting the case_default production.
	ExitCase_default(c *Case_defaultContext)

	// ExitCase_arg is called when exiting the case_arg production.
	ExitCase_arg(c *Case_argContext)

	// ExitColumnref is called when exiting the columnref production.
	ExitColumnref(c *ColumnrefContext)

	// ExitIndirection_el is called when exiting the indirection_el production.
	ExitIndirection_el(c *Indirection_elContext)

	// ExitSlice_bound_ is called when exiting the slice_bound_ production.
	ExitSlice_bound_(c *Slice_bound_Context)

	// ExitIndirection is called when exiting the indirection production.
	ExitIndirection(c *IndirectionContext)

	// ExitOpt_indirection is called when exiting the opt_indirection production.
	ExitOpt_indirection(c *Opt_indirectionContext)

	// ExitJson_passing_clause is called when exiting the json_passing_clause production.
	ExitJson_passing_clause(c *Json_passing_clauseContext)

	// ExitJson_arguments is called when exiting the json_arguments production.
	ExitJson_arguments(c *Json_argumentsContext)

	// ExitJson_argument is called when exiting the json_argument production.
	ExitJson_argument(c *Json_argumentContext)

	// ExitJson_wrapper_behavior is called when exiting the json_wrapper_behavior production.
	ExitJson_wrapper_behavior(c *Json_wrapper_behaviorContext)

	// ExitJson_behavior is called when exiting the json_behavior production.
	ExitJson_behavior(c *Json_behaviorContext)

	// ExitJson_behavior_type is called when exiting the json_behavior_type production.
	ExitJson_behavior_type(c *Json_behavior_typeContext)

	// ExitJson_behavior_clause is called when exiting the json_behavior_clause production.
	ExitJson_behavior_clause(c *Json_behavior_clauseContext)

	// ExitJson_on_error_clause is called when exiting the json_on_error_clause production.
	ExitJson_on_error_clause(c *Json_on_error_clauseContext)

	// ExitJson_value_expr is called when exiting the json_value_expr production.
	ExitJson_value_expr(c *Json_value_exprContext)

	// ExitJson_format_clause is called when exiting the json_format_clause production.
	ExitJson_format_clause(c *Json_format_clauseContext)

	// ExitJson_quotes_clause is called when exiting the json_quotes_clause production.
	ExitJson_quotes_clause(c *Json_quotes_clauseContext)

	// ExitJson_returning_clause is called when exiting the json_returning_clause production.
	ExitJson_returning_clause(c *Json_returning_clauseContext)

	// ExitJson_predicate_type_constraint is called when exiting the json_predicate_type_constraint production.
	ExitJson_predicate_type_constraint(c *Json_predicate_type_constraintContext)

	// ExitJson_key_uniqueness_constraint is called when exiting the json_key_uniqueness_constraint production.
	ExitJson_key_uniqueness_constraint(c *Json_key_uniqueness_constraintContext)

	// ExitJson_name_and_value_list is called when exiting the json_name_and_value_list production.
	ExitJson_name_and_value_list(c *Json_name_and_value_listContext)

	// ExitJson_name_and_value is called when exiting the json_name_and_value production.
	ExitJson_name_and_value(c *Json_name_and_valueContext)

	// ExitJson_object_constructor_null_clause is called when exiting the json_object_constructor_null_clause production.
	ExitJson_object_constructor_null_clause(c *Json_object_constructor_null_clauseContext)

	// ExitJson_array_constructor_null_clause is called when exiting the json_array_constructor_null_clause production.
	ExitJson_array_constructor_null_clause(c *Json_array_constructor_null_clauseContext)

	// ExitJson_value_expr_list is called when exiting the json_value_expr_list production.
	ExitJson_value_expr_list(c *Json_value_expr_listContext)

	// ExitJson_aggregate_func is called when exiting the json_aggregate_func production.
	ExitJson_aggregate_func(c *Json_aggregate_funcContext)

	// ExitJson_array_aggregate_order_by_clause is called when exiting the json_array_aggregate_order_by_clause production.
	ExitJson_array_aggregate_order_by_clause(c *Json_array_aggregate_order_by_clauseContext)

	// ExitTarget_list_ is called when exiting the target_list_ production.
	ExitTarget_list_(c *Target_list_Context)

	// ExitTarget_list is called when exiting the target_list production.
	ExitTarget_list(c *Target_listContext)

	// ExitTarget_label is called when exiting the target_label production.
	ExitTarget_label(c *Target_labelContext)

	// ExitTarget_star is called when exiting the target_star production.
	ExitTarget_star(c *Target_starContext)

	// ExitQualified_name_list is called when exiting the qualified_name_list production.
	ExitQualified_name_list(c *Qualified_name_listContext)

	// ExitQualified_name is called when exiting the qualified_name production.
	ExitQualified_name(c *Qualified_nameContext)

	// ExitName_list is called when exiting the name_list production.
	ExitName_list(c *Name_listContext)

	// ExitName is called when exiting the name production.
	ExitName(c *NameContext)

	// ExitAttr_name is called when exiting the attr_name production.
	ExitAttr_name(c *Attr_nameContext)

	// ExitFile_name is called when exiting the file_name production.
	ExitFile_name(c *File_nameContext)

	// ExitFunc_name is called when exiting the func_name production.
	ExitFunc_name(c *Func_nameContext)

	// ExitAexprconst is called when exiting the aexprconst production.
	ExitAexprconst(c *AexprconstContext)

	// ExitXconst is called when exiting the xconst production.
	ExitXconst(c *XconstContext)

	// ExitBconst is called when exiting the bconst production.
	ExitBconst(c *BconstContext)

	// ExitFconst is called when exiting the fconst production.
	ExitFconst(c *FconstContext)

	// ExitIconst is called when exiting the iconst production.
	ExitIconst(c *IconstContext)

	// ExitSconst is called when exiting the sconst production.
	ExitSconst(c *SconstContext)

	// ExitAnysconst is called when exiting the anysconst production.
	ExitAnysconst(c *AnysconstContext)

	// ExitUescape_ is called when exiting the uescape_ production.
	ExitUescape_(c *Uescape_Context)

	// ExitSignediconst is called when exiting the signediconst production.
	ExitSignediconst(c *SignediconstContext)

	// ExitRoleid is called when exiting the roleid production.
	ExitRoleid(c *RoleidContext)

	// ExitRolespec is called when exiting the rolespec production.
	ExitRolespec(c *RolespecContext)

	// ExitRole_list is called when exiting the role_list production.
	ExitRole_list(c *Role_listContext)

	// ExitColid is called when exiting the colid production.
	ExitColid(c *ColidContext)

	// ExitType_function_name is called when exiting the type_function_name production.
	ExitType_function_name(c *Type_function_nameContext)

	// ExitNonreservedword is called when exiting the nonreservedword production.
	ExitNonreservedword(c *NonreservedwordContext)

	// ExitColLabel is called when exiting the colLabel production.
	ExitColLabel(c *ColLabelContext)

	// ExitBareColLabel is called when exiting the bareColLabel production.
	ExitBareColLabel(c *BareColLabelContext)

	// ExitUnreserved_keyword is called when exiting the unreserved_keyword production.
	ExitUnreserved_keyword(c *Unreserved_keywordContext)

	// ExitCol_name_keyword is called when exiting the col_name_keyword production.
	ExitCol_name_keyword(c *Col_name_keywordContext)

	// ExitType_func_name_keyword is called when exiting the type_func_name_keyword production.
	ExitType_func_name_keyword(c *Type_func_name_keywordContext)

	// ExitReserved_keyword is called when exiting the reserved_keyword production.
	ExitReserved_keyword(c *Reserved_keywordContext)

	// ExitBare_label_keyword is called when exiting the bare_label_keyword production.
	ExitBare_label_keyword(c *Bare_label_keywordContext)

	// ExitAny_identifier is called when exiting the any_identifier production.
	ExitAny_identifier(c *Any_identifierContext)

	// ExitIdentifier is called when exiting the identifier production.
	ExitIdentifier(c *IdentifierContext)
}
