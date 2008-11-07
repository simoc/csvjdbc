/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2001  Jonathan Ackerman

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package test.org.relique.jdbc.csv;

import java.util.Map;

import org.relique.jdbc.csv.SqlParser;
import junit.framework.*;

/**
 * This class is used to test the SqlParser class.
 * 
 * @author Jonathan Ackerman
 * @version $Id: TestSqlParser.java,v 1.4 2008/11/07 11:29:30 mfrasca Exp $
 */
public class TestSqlParser extends TestCase {
	public TestSqlParser(String name) {
		super(name);
	}

	public void testParserSimple() throws Exception {
		SqlParser parser = new SqlParser();

		parser.parse("SELECT location, parameter, ts, waarde, unit FROM total");
		assertTrue("Incorrect table name", parser.getTableName().equals("total"));

		String[] cols = parser.getColumnNames();
		assertTrue("Incorrect Column Count", cols.length == 5);

		assertEquals("Incorrect Column Name Col 0", cols[0].toLowerCase(), "location");
		assertEquals("Incorrect Column Name Col 1", cols[1].toLowerCase(), "parameter");
		assertEquals("Incorrect Column Name Col 2", cols[2].toLowerCase(), "ts");
		assertEquals("Incorrect Column Name Col 3", cols[3].toLowerCase(), "waarde");
		assertEquals("Incorrect Column Name Col 4", cols[4].toLowerCase(), "unit");
	}

	
	
	public void testParser() throws Exception {
		SqlParser parser = new SqlParser();

		parser.parse("SELECT FLD_A,FLD_B, TEST, H FROM test");
		assertTrue("Incorrect table name", parser.getTableName().equals("test"));

		String[] cols = parser.getColumnNames();
		assertTrue("Incorrect Column Count", cols.length == 4);

		assertTrue("Incorrect Column Name Col 0", cols[0].equals("FLD_A"));
		assertTrue("Incorrect Column Name Col 1", cols[1].equals("FLD_B"));
		assertTrue("Incorrect Column Name Col 2", cols[2].equals("TEST"));
		assertTrue("Incorrect Column Name Col 3", cols[3].equals("H"));
	}

	public void testLiteralAsAlias() throws Exception {
		SqlParser parser = new SqlParser();

		parser.parse("SELECT 'abc' as FLD_A, 123 as FLD_B FROM test");
		assertTrue("Incorrect table name", parser.getTableName().equals("test"));

		String[] cols = parser.getColumnNames();
		assertTrue("Incorrect Column Count", cols.length == 2);

		assertTrue("Column Name Col 0 '"+cols[0]+"' is not equal FLD_A", cols[0].equalsIgnoreCase("fld_a"));
		assertTrue("Column Name Col 1 '"+cols[1]+"' is not equal FLD_B", cols[1].equalsIgnoreCase("FLD_B"));
	}

	public void testFieldAsAlias() throws Exception {
		SqlParser parser = new SqlParser();

		parser.parse("SELECT abc as FLD_A, eee as FLD_B FROM test");
		assertTrue("Incorrect table name", parser.getTableName().equals("test"));

		String[] cols = parser.getColumnNames();
		assertTrue("Incorrect Column Count", cols.length == 2);

		assertTrue("Column Name Col 0 '"+cols[0]+"' is not equal FLD_A", cols[0].equalsIgnoreCase("fld_a"));
		assertTrue("Column Name Col 1 '"+cols[1]+"' is not equal FLD_B", cols[1].equalsIgnoreCase("FLD_B"));
	}

	/**
	 * this case is only partially decoded by the parser...
	 * @throws Exception
	 */
	public void testAllColumns() throws Exception {
		SqlParser parser = new SqlParser();

		parser.parse("SELECT * FROM test");
		assertTrue("Incorrect table name", parser.getTableName().equals("test"));

		String[] cols = parser.getColumnNames();
		assertTrue("Incorrect Column Count "+cols.length, cols.length == 1);
	}

	/**
	 * Test that where conditions are handled correctly
	 * @throws Exception 
	 */
	public void testWhereCorrect() throws Exception {
		SqlParser parser = new SqlParser();
		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_A = 20");
		assertEquals("Incorrect table name", "test", parser.getTableName());
		assertEquals("Incorrect WHERE column index", 0, parser.getWhereColumn());
		assertEquals("Incorrect WHERE value", "20", parser.getWhereValue());

		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_A = '20'");
		assertEquals("Incorrect table name", "test", parser.getTableName());
		assertEquals("Incorrect WHERE column index", 0, parser.getWhereColumn());
		assertEquals("Incorrect WHERE value", "20", parser.getWhereValue());

		parser.parse("SELECT FLD_A,FLD_B FROM test WHERE FLD_A =20");
		assertEquals("Incorrect table name", "test", parser.getTableName());
		assertEquals("Incorrect WHERE column index", 0, parser.getWhereColumn());
		assertEquals("Incorrect WHERE value", "20", parser.getWhereValue());

		parser.parse("SELECT FLD_A FROM test WHERE FLD_A=20");
		assertEquals("Incorrect table name", "test", parser.getTableName());
		assertEquals("Incorrect WHERE column index", 0, parser.getWhereColumn());
		assertEquals("Incorrect WHERE value", "20", parser.getWhereValue());

		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_B='Test Me'");
		assertEquals("Incorrect table name", "test", parser.getTableName());
		assertEquals("Incorrect WHERE column index", 1, parser.getWhereColumn());
		assertEquals("Incorrect WHERE value", "Test Me", parser.getWhereValue());
	}

	/**
	 * Test that where conditions with AND operator are handled correctly
	 * @throws Exception 
	 */
	public void testWhereAnd() throws Exception {
		SqlParser parser = new SqlParser();
		Map whereClause;
		Map expr1;
		Map expr2;

		parser.parse("SELECT * FROM test WHERE FLD_A = '20' AND FLD_B = 'AA'");
		assertEquals("Incorrect table name", "test", parser.getTableName());
		whereClause = parser.getWhereClause();

		assertEquals("Incorrect WHERE operator (level1)", "AND",
				(String) whereClause.get("operator"));
		expr1 = (Map) whereClause.get("expr1");
		expr2 = (Map) whereClause.get("expr2");
		assertNotNull("Incorrect WHERE expr1 (level1)", expr1);
		assertNotNull("Incorrect WHERE expr2 (level1)", expr2);

		assertEquals("Incorrect WHERE column name", "FLD_A", (String) expr1
				.get("columnName"));
		assertEquals("Incorrect WHERE value", "20", (String) expr1.get("value"));
		assertEquals("Incorrect WHERE operator", "=", (String) expr1
				.get("operator"));

		Map expr21 = (Map) expr2.get("expr1");
		assertNotNull("Incorrect WHERE expr1 (level2)", expr21);
		assertEquals("Incorrect WHERE column name", "FLD_B", (String) expr21
				.get("columnName"));
		assertEquals("Incorrect WHERE value", "AA", (String) expr21
				.get("value"));
		assertEquals("Incorrect WHERE operator", "=", (String) expr21
				.get("operator"));

	}
}