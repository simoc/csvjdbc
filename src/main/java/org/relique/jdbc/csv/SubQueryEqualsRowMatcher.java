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
import java.util.ArrayList;

public class SubQueryEqualsRowMatcher implements SubQueryRowMatcher
{
	ArrayList<Object> values = new ArrayList<Object>();

	public boolean matches(Object expr) throws SQLException
	{
		/*
		 * For WHERE X = (SELECT ...) type sub-query we expect a
		 * maximum of one matching row.  Stop if we get more than
		 * one row, so parent/out SQL statement can throw an
		 * SQLException.
		 */
		values.add(expr);
		if (values.size() > 1)
			return true;
		else
			return false;
	}
	
	public ArrayList<Object> getValues()
	{
		return values;
	}
}
