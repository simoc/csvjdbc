/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.sql.*;

/**
 *This class will implement the DatabaseMetaData interface for the CsvJdbc driver.
 * This is mostly stub.
 *
 * @author     Jonathan Ackerman
 * @created    25 November 2001
 * @version    $Id: CsvDatabaseMetaData.java,v 1.3 2004/08/09 05:02:03 jackerm Exp $
 */
public class CsvDatabaseMetaData implements DatabaseMetaData
{
	private Connection createdByConnection;

	public CsvDatabaseMetaData(Connection createdByConnection)
	{
		this.createdByConnection = createdByConnection;
	}

	public boolean allProceduresAreCallable() throws SQLException
	{
		return true;
	}

	public boolean allTablesAreSelectable() throws SQLException
	{
		return true;
	}

	public boolean dataDefinitionCausesTransactionCommit() throws SQLException
	{
		return false;
	}

	public boolean dataDefinitionIgnoredInTransactions() throws SQLException
	{
		return false;
	}

	public boolean deletesAreDetected(int arg0) throws SQLException
	{
		return false;
	}

	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
	{
		return false;
	}

	public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getAttributes(String,String,String,String) unsupported");
	}

	public ResultSet getBestRowIdentifier(String arg0, String arg1, String arg2, int arg3, boolean arg4) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getBestRowIdentifier(String,String,String,int,boolean) unsupported");
	}

	public ResultSet getCatalogs() throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getCatalogs() unsupported");
	}

	public String getCatalogSeparator() throws SQLException
	{
		return ".";
	}

	public String getCatalogTerm() throws SQLException
	{
		return "catalog";
	}

	public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2, String arg3) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getColumnPrivileges(String,String,String,String) unsupported");
	}

	public ResultSet getColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getColumns(String,String,String,String) unsupported");
	}

	public Connection getConnection() throws SQLException
	{
		return createdByConnection;
	}

	public ResultSet getCrossReference(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5)
		throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getCrossReference(String,String,String,String,String,String) unsupported");
	}

	public int getDatabaseMajorVersion() throws SQLException
	{
		return 1;
	}

	public int getDatabaseMinorVersion() throws SQLException
	{
		return 0;
	}

	public String getDatabaseProductName() throws SQLException
	{
		return "CsvJdbc";
	}

	public String getDatabaseProductVersion() throws SQLException
	{
		return "1";
	}

	public int getDefaultTransactionIsolation() throws SQLException
	{
		return java.sql.Connection.TRANSACTION_NONE;
	}

	public int getDriverMajorVersion()
	{
		return 1;
	}

	public int getDriverMinorVersion()
	{
		return 0;
	}

	public String getDriverName() throws SQLException
	{
		return "CsvJdbc";
	}

	public String getDriverVersion() throws SQLException
	{
		return "1";
	}

	public ResultSet getExportedKeys(String arg0, String arg1, String arg2) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getExportedKeys(String,String,String) unsupported");
	}

	public String getExtraNameCharacters() throws SQLException
	{
		return "";
	}

	public String getIdentifierQuoteString() throws SQLException
	{
		return " ";
	}

	public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getImportedKeys(String,String,String) unsupported");
	}

	public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getIndexInfo(String,String,String,boolean,boolean) unsupported");
	}

	public int getJDBCMajorVersion() throws SQLException
	{
		return 3;
	}

	public int getJDBCMinorVersion() throws SQLException
	{
		return 0;
	}

	public int getMaxBinaryLiteralLength() throws SQLException
	{
		return 0;
	}

	public int getMaxCatalogNameLength() throws SQLException
	{
		return 0;
	}

	public int getMaxCharLiteralLength() throws SQLException
	{
		return 0;
	}

	public int getMaxColumnNameLength() throws SQLException
	{
		return 0;
	}

	public int getMaxColumnsInGroupBy() throws SQLException
	{
		return 0;
	}

	public int getMaxColumnsInIndex() throws SQLException
	{
		return 0;
	}

	public int getMaxColumnsInOrderBy() throws SQLException
	{
		return 0;
	}

	public int getMaxColumnsInSelect() throws SQLException
	{
		return 0;
	}

	public int getMaxColumnsInTable() throws SQLException
	{
		return 0;
	}

	public int getMaxConnections() throws SQLException
	{
		return 0;
	}

	public int getMaxCursorNameLength() throws SQLException
	{
		return 0;
	}

	public int getMaxIndexLength() throws SQLException
	{
		return 0;
	}

	public int getMaxProcedureNameLength() throws SQLException
	{
		return 0;
	}

	public int getMaxRowSize() throws SQLException
	{
		return 0;
	}

	public int getMaxSchemaNameLength() throws SQLException
	{
		return 0;
	}

	public int getMaxStatementLength() throws SQLException
	{
		return 0;
	}

	public int getMaxStatements() throws SQLException
	{
		return 0;
	}

	public int getMaxTableNameLength() throws SQLException
	{
		return 0;
	}

	public int getMaxTablesInSelect() throws SQLException
	{
		return 0;
	}

	public int getMaxUserNameLength() throws SQLException
	{
		return 0;
	}

	public String getNumericFunctions() throws SQLException
	{
		return "";
	}

	public ResultSet getPrimaryKeys(String arg0, String arg1, String arg2) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getPrimaryKeys(String,String,String) unsupported");
	}

	public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getProcedureColumns(String,String,String,String) unsupported");
	}

	public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getProcedures(String,String,String) unsupported");
	}

	public String getProcedureTerm() throws SQLException
	{
		return "procedure";
	}

	public int getResultSetHoldability() throws SQLException
	{
		return java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	public ResultSet getSchemas() throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getSchemas() unsupported");
	}

	public String getSchemaTerm() throws SQLException
	{
		return "schema";
	}

	public String getSearchStringEscape() throws SQLException
	{
		return "_";
	}

	public String getSQLKeywords() throws SQLException
	{
		return "";
	}

	public int getSQLStateType() throws SQLException
	{
		return sqlStateSQL99;
	}

	public String getStringFunctions() throws SQLException
	{
		return "";
	}

	public ResultSet getSuperTables(String arg0, String arg1, String arg2) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getSchemas(String,String,String) unsupported");
	}

	public ResultSet getSuperTypes(String arg0, String arg1, String arg2) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getSchemas(String,String,String) unsupported");
	}

	public String getSystemFunctions() throws SQLException
	{
		return "";
	}

	public ResultSet getTablePrivileges(String arg0, String arg1, String arg2) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getTablePrivileges(String,String,String) unsupported");
	}

	public ResultSet getTables(String arg0, String arg1, String arg2, String[] arg3) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getTablePrivileges(String,String,String,String[]) unsupported");
	}

	public ResultSet getTableTypes() throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getTableTypes() unsupported");
	}

	public String getTimeDateFunctions() throws SQLException
	{
		return "";
	}

	public ResultSet getTypeInfo() throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getTypeInfo() unsupported");
	}

	public ResultSet getUDTs(String arg0, String arg1, String arg2, int[] arg3) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getUDTs(String,String,String,int[]) unsupported");
	}

	public String getURL() throws SQLException
	{
		return null;
	}

	public String getUserName() throws SQLException
	{
		return "unknown";
	}

	public ResultSet getVersionColumns(String arg0, String arg1, String arg2) throws SQLException
	{
		throw new UnsupportedOperationException("DatabaseMetaData.getVersionColumns(String,String,String) unsupported");
	}

	public boolean insertsAreDetected(int arg0) throws SQLException
	{
		return false;
	}

	public boolean isCatalogAtStart() throws SQLException
	{
		return true;
	}

	public boolean isReadOnly() throws SQLException
	{
		return true;
	}

	public boolean locatorsUpdateCopy() throws SQLException
	{
		return false;
	}

	public boolean nullPlusNonNullIsNull() throws SQLException
	{
		return true;
	}

	public boolean nullsAreSortedAtEnd() throws SQLException
	{
		return true;
	}

	public boolean nullsAreSortedAtStart() throws SQLException
	{
		return false;
	}

	public boolean nullsAreSortedHigh() throws SQLException
	{
		return true;
	}

	public boolean nullsAreSortedLow() throws SQLException
	{
		return false;
	}

	public boolean othersDeletesAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	public boolean othersInsertsAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	public boolean othersUpdatesAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	public boolean ownDeletesAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	public boolean ownInsertsAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	public boolean ownUpdatesAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	public boolean storesLowerCaseIdentifiers() throws SQLException
	{
		return false;
	}

	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	public boolean storesMixedCaseIdentifiers() throws SQLException
	{
		return true;
	}

	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
	{
		return true;
	}

	public boolean storesUpperCaseIdentifiers() throws SQLException
	{
		return false;
	}

	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	public boolean supportsAlterTableWithAddColumn() throws SQLException
	{
		return false;
	}

	public boolean supportsAlterTableWithDropColumn() throws SQLException
	{
		return false;
	}

	public boolean supportsANSI92EntryLevelSQL() throws SQLException
	{
		return false;
	}

	public boolean supportsANSI92FullSQL() throws SQLException
	{
		return false;
	}

	public boolean supportsANSI92IntermediateSQL() throws SQLException
	{
		return false;
	}

	public boolean supportsBatchUpdates() throws SQLException
	{
		return false;
	}

	public boolean supportsCatalogsInDataManipulation() throws SQLException
	{
		return false;
	}

	public boolean supportsCatalogsInIndexDefinitions() throws SQLException
	{
		return false;
	}

	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
	{
		return false;
	}

	public boolean supportsCatalogsInProcedureCalls() throws SQLException
	{
		return false;
	}

	public boolean supportsCatalogsInTableDefinitions() throws SQLException
	{
		return false;
	}

	public boolean supportsColumnAliasing() throws SQLException
	{
		return false;
	}

	public boolean supportsConvert() throws SQLException
	{
		return false;
	}

	public boolean supportsConvert(int arg0, int arg1) throws SQLException
	{
		return false;
	}

	public boolean supportsCoreSQLGrammar() throws SQLException
	{
		return false;
	}

	public boolean supportsCorrelatedSubqueries() throws SQLException
	{
		return false;
	}

	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
	{
		return false;
	}

	public boolean supportsDataManipulationTransactionsOnly() throws SQLException
	{
		return false;
	}

	public boolean supportsDifferentTableCorrelationNames() throws SQLException
	{
		return false;
	}

	public boolean supportsExpressionsInOrderBy() throws SQLException
	{
		return false;
	}

	public boolean supportsExtendedSQLGrammar() throws SQLException
	{
		return false;
	}

	public boolean supportsFullOuterJoins() throws SQLException
	{
		return false;
	}

	public boolean supportsGetGeneratedKeys() throws SQLException
	{
		return false;
	}

	public boolean supportsGroupBy() throws SQLException
	{
		return false;
	}

	public boolean supportsGroupByBeyondSelect() throws SQLException
	{
		return false;
	}

	public boolean supportsGroupByUnrelated() throws SQLException
	{
		return false;
	}

	public boolean supportsIntegrityEnhancementFacility() throws SQLException
	{
		return false;
	}

	public boolean supportsLikeEscapeClause() throws SQLException
	{
		return false;
	}

	public boolean supportsLimitedOuterJoins() throws SQLException
	{
		return false;
	}

	public boolean supportsMinimumSQLGrammar() throws SQLException
	{
		return false;
	}

	public boolean supportsMixedCaseIdentifiers() throws SQLException
	{
		return false;
	}

	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	public boolean supportsMultipleOpenResults() throws SQLException
	{
		return true;
	}

	public boolean supportsMultipleResultSets() throws SQLException
	{
		return true;
	}

	public boolean supportsMultipleTransactions() throws SQLException
	{
		return false;
	}

	public boolean supportsNamedParameters() throws SQLException
	{
		return false;
	}

	public boolean supportsNonNullableColumns() throws SQLException
	{
		return false;
	}

	public boolean supportsOpenCursorsAcrossCommit() throws SQLException
	{
		return false;
	}

	public boolean supportsOpenCursorsAcrossRollback() throws SQLException
	{
		return false;
	}

	public boolean supportsOpenStatementsAcrossCommit() throws SQLException
	{
		return false;
	}

	public boolean supportsOpenStatementsAcrossRollback() throws SQLException
	{
		return false;
	}

	public boolean supportsOrderByUnrelated() throws SQLException
	{
		return false;
	}

	public boolean supportsOuterJoins() throws SQLException
	{
		return false;
	}

	public boolean supportsPositionedDelete() throws SQLException
	{
		return false;
	}

	public boolean supportsPositionedUpdate() throws SQLException
	{
		return false;
	}

	public boolean supportsResultSetConcurrency(int arg0, int arg1) throws SQLException
	{
		return false;
	}

	public boolean supportsResultSetHoldability(int arg0) throws SQLException
	{
		return false;
	}

	public boolean supportsResultSetType(int type) throws SQLException
	{
		if (type == ResultSet.TYPE_FORWARD_ONLY)
		{
			return true;
		}

		if (type == ResultSet.TYPE_SCROLL_SENSITIVE)
		{
			return true;
		}

		return false;
	}

	public boolean supportsSavepoints() throws SQLException
	{
		return false;
	}

	public boolean supportsSchemasInDataManipulation() throws SQLException
	{
		return false;
	}

	public boolean supportsSchemasInIndexDefinitions() throws SQLException
	{
		return false;
	}

	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
	{
		return false;
	}

	public boolean supportsSchemasInProcedureCalls() throws SQLException
	{
		return false;
	}

	public boolean supportsSchemasInTableDefinitions() throws SQLException
	{
		return false;
	}

	public boolean supportsSelectForUpdate() throws SQLException
	{
		return false;
	}

	public boolean supportsStatementPooling() throws SQLException
	{
		return false;
	}

	public boolean supportsStoredProcedures() throws SQLException
	{
		return false;
	}

	public boolean supportsSubqueriesInComparisons() throws SQLException
	{
		return false;
	}

	public boolean supportsSubqueriesInExists() throws SQLException
	{
		return false;
	}

	public boolean supportsSubqueriesInIns() throws SQLException
	{
		return false;
	}

	public boolean supportsSubqueriesInQuantifieds() throws SQLException
	{
		return false;
	}

	public boolean supportsTableCorrelationNames() throws SQLException
	{
		return false;
	}

	public boolean supportsTransactionIsolationLevel(int arg0) throws SQLException
	{
		return false;
	}

	public boolean supportsTransactions() throws SQLException
	{
		return false;
	}

	public boolean supportsUnion() throws SQLException
	{
		return false;
	}

	public boolean supportsUnionAll() throws SQLException
	{
		return false;
	}

	public boolean updatesAreDetected(int arg0) throws SQLException
	{
		return false;
	}

	public boolean usesLocalFilePerTable() throws SQLException
	{
		return false;
	}

	public boolean usesLocalFiles() throws SQLException
	{
		return false;
	}

}
