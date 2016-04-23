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
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.relique.io.ListDataReader;

/**
 * This class implements the java.sql.DatabaseMetaData JDBC interface for the
 * CsvJdbc driver.
 */
public class CsvDatabaseMetaData implements DatabaseMetaData
{
	private Connection createdByConnection;
	private CsvStatement internalStatement = null;

	public CsvDatabaseMetaData(Connection createdByConnection)
	{
		this.createdByConnection = createdByConnection;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException
	{
		return true;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException
	{
		return true;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException
	{
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean deletesAreDetected(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
	{
		return false;
	}

	@Override
	public ResultSet getAttributes(String arg0, String arg1, String arg2,
			String arg3) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": DatabaseMetaData.getAttributes(String,String,String,String)");
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException
	{
		String columnNames = "SCOPE,COLUMN_NAME,DATA_TYPE,TYPE_NAME,COLUMN_SIZE,BUFFER_LENGTH,DECIMAL_DIGITS,PSEUDO_COLUMN";
		String columnTypes = "Short,String,Integer,String,Integer,Integer,Short,Short";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes,
				columnValues);
		return retval;
	}

	@Override
	public ResultSet getCatalogs() throws SQLException
	{
		String columnNames = "TABLE_CAT";
		String columnTypes = "String";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes,
				columnValues);
		return retval;
	}

	@Override
	public String getCatalogSeparator() throws SQLException
	{
		return ".";
	}

	@Override
	public String getCatalogTerm() throws SQLException
	{
		return "catalog";
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException
	{
		return getTablePrivileges(catalog, schema, table);
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
		String tableNamePattern, String columnNamePattern) throws SQLException
	{
		String columnNames = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,DATA_TYPE,TYPE_NAME,COLUMN_SIZE,BUFFER_LENGTH,"
			+ "DECIMAL_DIGITS,NUM_PREC_RADIX,NULLABLE,REMARKS,COLUMN_DEF,SQL_DATA_TYPE,SQL_DATETIME_SUB,CHAR_OCTET_LENGTH,"
			+ "ORDINAL_POSITION,IS_NULLABLE,SCOPE_CATLOG,SCOPE_SCHEMA,SCOPE_TABLE,SOURCE_DATA_TYPE,IS_AUTOINCREMENT";
		String columnTypes = "String,String,String,String,Integer,String,Integer,Integer,Integer,Integer,Integer,"
			+ "String,String,Integer,Integer,Integer,Integer,String,String,String,String,Short,String";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet resultSet = null;
		ResultSet resultSet2 = null;
		try
		{			
			if (internalStatement == null)
				internalStatement = (CsvStatement) createdByConnection.createStatement();

			/*
			 * Find tables matching the pattern.
			 */
			resultSet = getTables(catalog, schemaPattern, tableNamePattern, null);
			while (resultSet.next())
			{				
				String tableName = resultSet.getString(3);

				resultSet2 = internalStatement.executeQuery("SELECT * FROM \"" + tableName + "\"");
				ResultSetMetaData metadata = resultSet2.getMetaData();
				int nColumns = metadata.getColumnCount();
				Integer columnSize = Integer.valueOf(Short.MAX_VALUE);
				Integer decimalDigits = Integer.valueOf(Short.MAX_VALUE);
				Integer zero = Integer.valueOf(0);
				Integer radix = Integer.valueOf(10);
				Integer nullable = Integer.valueOf(columnNullable);
				String remarks = null;
				String defaultValue = null;
	
				for (int i = 0; i < nColumns; i++)
				{
					String columnName = metadata.getColumnName(i + 1);

					/*
					 * Only add columns matching the column pattern.
					 */
					if (columnNamePattern == null ||
						LikePattern.matches(columnNamePattern, LikePattern.DEFAULT_ESCAPE_STRING, columnName))
					{
						int columnType = metadata.getColumnType(i + 1);
						String columnTypeName = metadata.getColumnTypeName(i + 1);
						Object data[] = { null, null, tableName, columnName,
							Integer.valueOf(columnType), columnTypeName,
							columnSize, zero, decimalDigits, radix, nullable,
							remarks, defaultValue, zero, zero, columnSize,
							Integer.valueOf(i + 1), "YES", null, null, null, null,
							"NO" };
						columnValues.add(data);
					}
				}
				resultSet2.close();
				resultSet2 = null;
			}
		}
		finally
		{
			if (resultSet2 != null)
				resultSet2.close();
			if (resultSet != null)
				resultSet.close();
		}
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		return createdByConnection;
	}

	@Override
	public ResultSet getCrossReference(String arg0, String arg1, String arg2,
			String arg3, String arg4, String arg5) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": DatabaseMetaData.getCrossReference(String,String,String,String,String,String)");
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException
	{
		return 1;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException
	{
		return 0;
	}

	@Override
	public String getDatabaseProductName() throws SQLException
	{
		return "CsvJdbc";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException
	{
		return "1";
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException
	{
		return java.sql.Connection.TRANSACTION_NONE;
	}

	@Override
	public int getDriverMajorVersion()
	{
		return 1;
	}

	@Override
	public int getDriverMinorVersion()
	{
		return 0;
	}

	@Override
	public String getDriverName() throws SQLException
	{
		return "CsvJdbc";
	}

	@Override
	public String getDriverVersion() throws SQLException
	{
		return "1";
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException
	{
		String columnNames = "PKTABLE_CAT,PKTABLE_SCHEM,PKTABLE_NAME,PKCOLUMN_NAME,FKTABLE_CAT,FKTABLE_SCHEM,"
				+ "FKTABLE_NAME,FKCOLUMN_NAME,KEY_SEQ,UPDATE_RULE,DELETE_RULE,FK_NAME,PK_NAME,DEFERRABILITY";
		String columnTypes = "String,String,String,String,String,String,String,String,Short,Short,Short,String,String,Short";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes,
				columnValues);
		return retval;
	}

	@Override
	public String getExtraNameCharacters() throws SQLException
	{
		return "";
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException
	{
		/*
		 * Identifiers enclosed in double-quotes will not be
		 * interpreted as SQL.
		 */
		return "\"";
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException
	{
		return getExportedKeys(catalog, schema, table);
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException
	{
		String columnNames = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,NON_UNIQUE,INDEX_QUALIFIER,INDEX_NAME,TYPE," +
			"ORDINAL_POSITION,COLUMN_NAME,ASC_OR_DESC,CARDINALITY,PAGES,FILTER_CONDITION";
		String columnTypes = "String,String,String,Boolean,String,String,Short,Short,String,String,Integer,Integer,String";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException
	{
		return 3;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxStatementLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException
	{
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException
	{
		return 0;
	}

	@Override
	public String getNumericFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException
	{
		String columnNames = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,KEY_SEQ,PK_NAME";
		String columnTypes = "String,String,String,String,Short,String";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	@Override
	public ResultSet getProcedureColumns(String arg0, String arg1, String arg2,
			String arg3) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": DatabaseMetaData.getProcedureColumns(String,String,String,String)");
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException
	{
		String columnNames = "PROCEDURE_CAT,PROCEDURE_SCHEM,PROCEDURE_NAME,reserved4,reserved5,reserved6,REMARKS,PROCEDURE_TYPE,SPECIFIC_NAME";
		String columnTypes = "String,String,String,String,String,String,String,Short,String";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	@Override
	public String getProcedureTerm() throws SQLException
	{
		return "procedure";
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public ResultSet getSchemas() throws SQLException
	{
		String columnNames = "TABLE_SCHEM,TABLE_CATALOG";
		String columnTypes = "String,String";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes,
				columnValues);
		return retval;
	}

	@Override
	public String getSchemaTerm() throws SQLException
	{
		return "schema";
	}

	@Override
	public String getSearchStringEscape() throws SQLException
	{
		return "_";
	}

	@Override
	public String getSQLKeywords() throws SQLException
	{
		return "";
	}

	@Override
	public int getSQLStateType() throws SQLException
	{
		return sqlStateSQL99;
	}

	@Override
	public String getStringFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getSuperTables(String arg0, String arg1, String arg2)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": DatabaseMetaData.getSuperTables(String,String,String)");
	}

	@Override
	public ResultSet getSuperTypes(String arg0, String arg1, String arg2)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			"DatabaseMetaData.getSuperTypes(String,String,String)");
	}

	@Override
	public String getSystemFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException
	{
		String columnNames = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,GRANTOR,GRANTEE,PRIVILEGE,IS_GRANTABLE";
		String columnTypes = "String,String,String,String,String,String,String";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException
	{
		String columnNames = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,TABLE_TYPE," +
			"REMARKS,TYPE_CAT,TYPE_SCHEM,TYPE_NAME,SELF_REFERENCING_COL_NAME,REF_GENERATION";
		String columnTypes = "String,String,String,String,String,String,String,String,String,String";
		List<String> tableNames = ((CsvConnection) createdByConnection).getTableNames();
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>(tableNames.size());

		boolean typesMatch = false;

		if (types == null)
		{
			typesMatch = true;
		}
		else
		{
			for (int i = 0; i < types.length; i++)
			{
				if (types[i].equals("TABLE"))
					typesMatch = true;
			}
		}

		for (int i = 0; i < tableNames.size(); i++)
		{
			String tableName = tableNames.get(i);
			if (typesMatch && (tableNamePattern == null || LikePattern.matches(tableNamePattern, LikePattern.DEFAULT_ESCAPE_STRING, tableName)))
			{
				Object[] data = new Object[]{null, null, tableName, "TABLE", "",
					null, null, null, null, null};
				columnValues.add(data);
			}
		}
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	private ResultSet createResultSet(String columnNames, String columnTypes,
			List<Object[]> columnValues) throws SQLException
	{
		ListDataReader reader = new ListDataReader(columnNames.split(","),
			columnTypes.split(","), columnValues);
		ArrayList<Object[]> queryEnvironment = new ArrayList<Object[]>();
		queryEnvironment.add(new Object[]{"*", new AsteriskExpression("*")});
		ResultSet retval = null;

		try
		{
			if (internalStatement == null)
				internalStatement = (CsvStatement) createdByConnection.createStatement();
			retval = new CsvResultSet(internalStatement, reader, "",
				queryEnvironment, false, ResultSet.TYPE_FORWARD_ONLY, null, null, null, null, -1, 0,
				columnTypes, 0, new HashMap<String, Object>());
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e.getMessage());
		}
		return retval;
	}

	@Override
	public ResultSet getTableTypes() throws SQLException
	{
		String columnNames = "TABLE_TYPE";
		String columnTypes = "String";
		Object[] data = new Object[]{"TABLE"};
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		columnValues.add(data);
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	@Override
	public String getTimeDateFunctions() throws SQLException
	{
		return "";
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException
	{
		String columnNames = "TYPE_NAME,DATA_TYPE,PRECISION,LITERAL_PREFIX,LITERAL_SUFFIX,CREATE_PARAMS," +
			"NULLABLE,CASE_SENSITIVE,SEARCHABLE,UNSIGNED_ATTRIBUTE,FIXED_PREC_SCALE,AUTO_INCREMENT," +
			"LOCAL_TYPE_NAME,MINIMUM_SCALE,MAXIMUM_SCALE,SQL_DATA_TYPE,SQL_DATETIME_SUB,NUM_PREC_RADIX";
		String columnTypes = "String,Integer,Integer,String,String,String,Short,Boolean,Short," +
			"Boolean,Boolean,Boolean,String,Short,Short,Integer,Integer,Integer";
		List<Object[]> columnValues = StringConverter.getTypeInfo();
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern,
		String typeNamePattern, int[] types) throws SQLException
	{
		String columnNames = "TYPE_CAT,TYPE_SCHEM,TYPE_NAME,CLASS_NAME,DATA_TYPE,REMARKS,BASE_TYPE";
		String columnTypes = "String,String,String,String,Integer,String,Short";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}

	@Override
	public String getURL() throws SQLException
	{
		return ((CsvConnection)createdByConnection).getURL();
	}

	@Override
	public String getUserName() throws SQLException
	{
		return "unknown";
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException
	{
		return getBestRowIdentifier(catalog, schema, table, bestRowTemporary, false);
	}

	@Override
	public boolean insertsAreDetected(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException
	{
		return true;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		return true;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException
	{
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException
	{
		return true;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException
	{
		return true;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException
	{
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException
	{
		return true;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException
	{
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException
	{
		return true;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
	{
		return true;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsConvert(int arg0, int arg1) throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(int arg0, int arg1)
			throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException
	{
		return (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}

	@Override
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

	@Override
	public boolean supportsSavepoints() throws SQLException
	{
		return true;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int arg0)
			throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException
	{
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException
	{
		return false;
	}

	@Override
	public boolean updatesAreDetected(int arg0) throws SQLException
	{
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException
	{
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException
	{
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	public boolean generatedKeyAlwaysReturned() throws SQLException
	{
		return false;
	}

	public ResultSet getPseudoColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException
	{

		String columnNames = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_SIZE,DECIMAL_DIGITS,NUM_PREC_RADIX,COLUMN_USAGE,REMARKS,CHAR_OCTET_LENGTH,IS_NULLABLE";
		String columnTypes = "String,String,String,String,Integer,Integer,Integer,Integer,String,String,Integer,String";
		ArrayList<Object[]> columnValues = new ArrayList<Object[]>();
		ResultSet retval = createResultSet(columnNames, columnTypes, columnValues);
		return retval;
	}
}
