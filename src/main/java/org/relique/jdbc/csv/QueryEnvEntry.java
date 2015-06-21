/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2008  Mario Frasca
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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.sql.SQLException;
import java.util.Map;

class QueryEnvEntry extends Expression
{
	String key;
	Expression expression;

	public QueryEnvEntry(String fieldName, Expression exp)
	{
		this.key = fieldName.toUpperCase();
		this.expression = exp;
	}
	public Object eval(Map<String, Object> env) throws SQLException
	{
		return expression.eval(env);
	}
	public String toString()
	{
		return key+": "+expression.toString();
	}
	public void resetAggregateFunctions()
	{
		expression.resetAggregateFunctions();
	}
}
