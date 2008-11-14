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

import java.util.HashMap;
import java.util.Map;

import org.relique.jdbc.csv.LogicalExpressionParser;
import org.relique.jdbc.csv.ParseException;
import org.relique.jdbc.csv.SqlParser;
import junit.framework.*;

/**
 * This class is used to test the SqlParser class.
 * 
 * @author Jonathan Ackerman
 * @version $Id: TestSqlParser.java,v 1.8 2008/11/14 15:54:54 mfrasca Exp $
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

		assertTrue("Column Name Col 0 '" + cols[0] + "' is not equal FLD_A", cols[0].equalsIgnoreCase("fld_a"));
		assertTrue("Column Name Col 1 '" + cols[1] + "' is not equal FLD_B", cols[1].equalsIgnoreCase("FLD_B"));
	}

	public void testFieldAsAlias() throws Exception {
		SqlParser parser = new SqlParser();

		parser.parse("SELECT abc as FLD_A, eee as FLD_B FROM test");
		assertTrue("Incorrect table name", parser.getTableName().equals("test"));

		String[] cols = parser.getColumnNames();
		assertTrue("Incorrect Column Count", cols.length == 2);

		assertTrue("Column Name Col 0 '" + cols[0] + "' is not equal FLD_A", cols[0].equalsIgnoreCase("fld_a"));
		assertTrue("Column Name Col 1 '" + cols[1] + "' is not equal FLD_B", cols[1].equalsIgnoreCase("FLD_B"));
	}

	/**
	 * this case is only partially decoded by the parser...
	 * 
	 * @throws Exception
	 */
	public void testAllColumns() throws Exception {
		SqlParser parser = new SqlParser();

		parser.parse("SELECT * FROM test");
		assertTrue("Incorrect table name", parser.getTableName().equals("test"));

		String[] cols = parser.getColumnNames();
		assertTrue("Incorrect Column Count " + cols.length, cols.length == 1);
	}

	/**
	 * Test that where conditions are handled correctly
	 * 
	 * @throws Exception
	 */
	public void testWhereCorrect() throws Exception {
		SqlParser parser = new SqlParser();
		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_A = 20");
		assertEquals("Incorrect table name", "test", parser.getTableName());

		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_A = '20'");
		assertEquals("Incorrect table name", "test", parser.getTableName());

		parser.parse("SELECT FLD_A,FLD_B FROM test WHERE FLD_A =20");
		assertEquals("Incorrect table name", "test", parser.getTableName());

		parser.parse("SELECT FLD_A FROM test WHERE FLD_A=20");
		assertEquals("Incorrect table name", "test", parser.getTableName());

		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_B='Test Me'");
		assertEquals("Incorrect table name", "test", parser.getTableName());
	}

	/**
	 * Test that where conditions with AND operator are parsed correctly
	 * 
	 * @throws Exception
	 */
	public void testWhereParsing() throws Exception {
		SqlParser parser = new SqlParser();
		LogicalExpressionParser whereClause;

		parser.parse("SELECT * FROM test WHERE A='20'");
		whereClause = parser.getWhereClause();
		assertNotNull("query has a WHERE clause", whereClause);
		assertEquals("Incorrect WHERE", "= [A] '20'",
				whereClause.toString());
		
		parser.parse("SELECT * FROM test WHERE A='20' AND B='AA'");
		whereClause = parser.getWhereClause();
		assertEquals("Incorrect WHERE", "AND = [A] '20' = [B] 'AA'",
				whereClause.toString());

		parser.parse("SELECT * FROM test WHERE A='20' OR B='AA'");
		whereClause = parser.getWhereClause();
		assertEquals("Incorrect WHERE", "OR = [A] '20' = [B] 'AA'",
				whereClause.toString());

		parser.parse("SELECT * FROM test WHERE A='20' OR B='AA' AND c=1");
		whereClause = parser.getWhereClause();
		assertEquals("Incorrect WHERE", "OR = [A] '20' AND = [B] 'AA' = [C] 1",
				whereClause.toString());

		parser.parse("SELECT * FROM test WHERE (A='20' OR B='AA') AND c=1");
		whereClause = parser.getWhereClause();
		assertEquals("Incorrect WHERE", "AND OR = [A] '20' = [B] 'AA' = [C] 1",
				whereClause.toString());
	}

	public void testWhereMoreParsing() throws Exception {
		SqlParser parser = new SqlParser();
		String query;

		try {
			query = "SELECT * FROM test WHERE FLD_A = '20' AND AND = 'AA'";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		} catch (ParseException e) {
		}

		try {
			query = "SELECT * FROM test WHERE = 'AA'";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		} catch (ParseException e) {
		}

		try {
			query = "SELECT * FROM test WHERE FLD_A = '20' = 'AA'";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		} catch (ParseException e) {
		}

		
		parser.parse("SELECT * FROM test WHERE B IS NULL");
		LogicalExpressionParser whereClause = parser.getWhereClause();
		assertEquals("Incorrect WHERE", "N [B]",
				whereClause.toString());
		parser.parse("SELECT * FROM test WHERE B BETWEEN '20' AND 'AA'");
		whereClause = parser.getWhereClause();
		assertEquals("Incorrect WHERE", "B [B] '20' 'AA'",
				whereClause.toString());
		parser.parse("SELECT * FROM test WHERE B LIKE '20 AND AA'");
		whereClause = parser.getWhereClause();
		assertEquals("Incorrect WHERE", "L [B] '20 AND AA'",
				whereClause.toString());

		parser.parse("SELECT * FROM test WHERE B IS NULL OR B BETWEEN '20' AND 'AA' AND B LIKE '20 AND AA'");
		whereClause = parser.getWhereClause();
		assertEquals("Incorrect WHERE", "OR N [B] AND B [B] '20' 'AA' L [B] '20 AND AA'",
				whereClause.toString());

		try {
			query = "SELECT * FROM test WHERE a=0 AND FLD_A";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		} catch (ParseException e) {
		}

	}

	/**
	 * Test that where conditions with AND operator are parsed correctly
	 * 
	 * @throws Exception
	 */
	public void testWhereEvaluating() throws Exception {
		SqlParser parser = new SqlParser();
		LogicalExpressionParser whereClause;
		Map env = new HashMap();

		parser.parse("SELECT * FROM test WHERE c=1");
		env.clear();
		env.put("C", new Integer("1"));
		assertEquals(true, parser.getWhereClause().eval(env));
		
		parser.parse("SELECT * FROM test WHERE c='1'");
		env.clear();
		env.put("C", new String("1"));
		assertEquals(true, parser.getWhereClause().eval(env));
		
		parser.parse("SELECT * FROM test WHERE (A='20' OR B='AA') AND c=1");
		whereClause = parser.getWhereClause();

		env.clear();
		env.put("A", new String("20"));
		env.put("B", new String("AA"));
		env.put("C", new Integer("1"));
		assertEquals(true, whereClause.eval(env));
		env.put("A", new Double("20"));
		assertEquals(true, whereClause.eval(env));
		env.put("B", new String(""));
		assertEquals(false, whereClause.eval(env));
	}

}