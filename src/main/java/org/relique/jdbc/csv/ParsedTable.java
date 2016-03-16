/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2014  Simon Chenery
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

class ParsedTable
{
	private JoinType joinType;
	private LogicalExpression joinClause; 
	private String tableName;
	private String tableAlias;
	private ParsedStatement derivedTableStatement;

	public ParsedTable(String tableName, String tableAlias)
	{
		this.joinType = JoinType.NONE;
		this.joinClause = null;
		this.tableName = tableName;
		this.tableAlias = tableAlias;
	}

	public ParsedTable(JoinType joinType, LogicalExpression joinClause,
		String tableName, String tableAlias)
	{
		this.joinType = joinType;
		this.joinClause = joinClause;
		this.tableName = tableName;
		this.tableAlias = tableAlias;
	}

	public ParsedTable(ParsedStatement parsedStatement, String tableAlias)
	{
		this.derivedTableStatement = parsedStatement;
		this.tableAlias = tableAlias;
	}

	public ParsedTable(JoinType joinType, LogicalExpression joinClause, ParsedTable joinedTable)
	{
		this.joinType = joinType;
		this.joinClause = joinClause;
		if (joinedTable.isDerivedTable())
			this.derivedTableStatement = joinedTable.getDerivedTableStatement();
		else
			this.tableName = joinedTable.getTableName();
		this.tableAlias = joinedTable.getTableAlias();
	}

	public JoinType getJoinType()
	{
		return joinType;
	}

	public LogicalExpression getJoinClause()
	{
		return joinClause;
	}

	public String getTableName()
	{
		return tableName;
	}

	public String getTableAlias()
	{
		return tableAlias;
	}

	public boolean isDerivedTable()
	{
		return derivedTableStatement != null;
	}

	public ParsedStatement getDerivedTableStatement()
	{
		return derivedTableStatement;
	}
}
