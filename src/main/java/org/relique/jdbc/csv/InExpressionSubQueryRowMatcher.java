/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2015  Simon Chenery
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

import java.sql.SQLException;
import java.util.Map;

public class InExpressionSubQueryRowMatcher implements SubQueryRowMatcher
{
	private Map<String, Object> env;
	private Comparable objValue;

	public InExpressionSubQueryRowMatcher(Map<String, Object> env, Comparable objValue)
	{
		this.env = env;
		this.objValue = objValue;
	}

	public boolean matches(Object expr) throws SQLException
	{
		/*
		 * Does this row from the sub-query match the IN expression value
		 * from the outer/parent SQL statement?
		 */
		Comparable exprValue = (Comparable)expr;
		Integer compared = RelopExpression.compare(objValue, exprValue, env);
		return (compared != null && compared.intValue() == 0);
	}
}
