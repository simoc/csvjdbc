/**
 * CsvJdbc - a JDBC driver for CSV files
 * Copyright (C) 2001  Jonathan Ackerman
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.StringReader;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * This class is used to test the SqlParser class.
 * 
 * @author Jonathan Ackerman
 */
public class TestSqlParser
{
	@Test
	public void testParserSimple() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("SELECT location, parameter, ts, waarde, unit FROM total");
		assertTrue(parser.getTableNames().get(0).equals("total"), "Incorrect table name");

		String[] colNames = parser.getColumnNames();
		assertTrue(colNames.length == 5, "Incorrect Column Count");

		assertEquals(colNames[0].toLowerCase(), "location", "Incorrect Column Name Col 0");
		assertEquals(colNames[1].toLowerCase(), "parameter", "Incorrect Column Name Col 1");
		assertEquals(colNames[2].toLowerCase(), "ts", "Incorrect Column Name Col 2");
		assertEquals(colNames[3].toLowerCase(), "waarde", "Incorrect Column Name Col 3");
		assertEquals(colNames[4].toLowerCase(), "unit", "Incorrect Column Name Col 4");

		parser.parse("SELECT location, parameter, ts, name.suffix as value FROM total");
		assertTrue(parser.getTableNames().get(0).equals("total"), "Incorrect table name");

		assertEquals(4, parser.getColumns().size(), "Incorrect Column Count");

		List<Object []> cols = parser.getColumns();
		assertEquals(4, cols.size(), "Incorrect Column Count");

		Object[] colSpec = cols.get(3);
		assertEquals("VALUE", colSpec[0].toString(), "Incorrect Column Name Col 3");
		assertEquals("[NAME.SUFFIX]", colSpec[1].toString(), "Incorrect Column Name Col 3");

		try
		{
			String query = "SELECT location!parameter FROM total";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		}
		catch (Exception e)
		{
		}

		parser.parse("SELECT location+parameter FROM total");
		colNames = parser.getColumnNames();
		assertEquals("+ [LOCATION] [PARAMETER]", colNames[0], "Incorrect Column Name Col 1");

		parser.parse("SELECT location-parameter FROM total");
		colNames = parser.getColumnNames();
		assertEquals("- [LOCATION] [PARAMETER]", colNames[0], "Incorrect Column Name Col 1");

		parser.parse("SELECT location*parameter FROM total");
		colNames = parser.getColumnNames();
		assertEquals("* [LOCATION] [PARAMETER]", colNames[0], "Incorrect Column Name Col 1");
	}

	@Test
	public void testParser() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("SELECT FLD_A,FLD_B, TEST, H FROM test");
		assertTrue(parser.getTableNames().get(0).equals("test"), "Incorrect table name");

		String[] cols = parser.getColumnNames();
		assertTrue(cols.length == 4, "Incorrect Column Count");

		assertTrue(cols[0].equals("FLD_A"), "Incorrect Column Name Col 0");
		assertTrue(cols[1].equals("FLD_B"), "Incorrect Column Name Col 1");
		assertTrue(cols[2].equals("TEST"), "Incorrect Column Name Col 2");
		assertTrue(cols[3].equals("H"), "Incorrect Column Name Col 3");
	}

	@Test
	public void testLiteralAsAlias() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("SELECT 'abc' as FLD_A, 123 as FLD_B FROM test");
		assertTrue(parser.getTableNames().get(0).equals("test"), "Incorrect table name");

		String[] cols = parser.getColumnNames();
		assertTrue(cols.length == 2, "Incorrect Column Count");

		assertTrue(cols[0].equalsIgnoreCase("fld_a"),
			"Column Name Col 0 '" + cols[0] + "' is not equal FLD_A");
		assertTrue(cols[1].equalsIgnoreCase("FLD_B"),
			"Column Name Col 1 '" + cols[1] + "' is not equal FLD_B");
	}

	@Test
	public void testFieldAsAlias() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("SELECT abc as FLD_A, eee as FLD_B FROM test");
		assertTrue(parser.getTableNames().get(0).equals("test"), "Incorrect table name");

		String[] cols = parser.getColumnNames();
		assertTrue(cols.length == 2, "Incorrect Column Count");

		assertTrue(cols[0].equalsIgnoreCase("fld_a"),
			"Column Name Col 0 '" + cols[0] + "' is not equal FLD_A");
		assertTrue(cols[1].equalsIgnoreCase("FLD_B"),
			"Column Name Col 1 '" + cols[1] + "' is not equal FLD_B");
	}

	/**
	 * this case is only partially decoded by the parser...
	 */
	@Test
	public void testAllColumns() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("SELECT * FROM test");
		assertTrue(parser.getTableNames().get(0).equals("test"), "Incorrect table name");

		String[] cols = parser.getColumnNames();
		assertEquals(1, cols.length, "Incorrect Column Count");
	}

	/**
	 * Test that where conditions are handled correctly
	 */
	@Test
	public void testWhereCorrect() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_A = 20");
		assertEquals("test", parser.getTableNames().get(0), "Incorrect table name");

		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_A = '20'");
		assertEquals("test", parser.getTableNames().get(0), "Incorrect table name");

		parser.parse("SELECT FLD_A,FLD_B FROM test WHERE FLD_A =20");
		assertEquals("test", parser.getTableNames().get(0), "Incorrect table name");

		parser.parse("SELECT FLD_A FROM test WHERE FLD_A=20");
		assertEquals("test", parser.getTableNames().get(0), "Incorrect table name");

		parser.parse("SELECT FLD_A, FLD_B FROM test WHERE FLD_B='Test Me'");
		assertEquals("test", parser.getTableNames().get(0), "Incorrect table name");
	}

	/**
	 * Test that where conditions with AND operator are parsed correctly
	 */
	@Test
	public void testWhereParsing() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Expression whereClause;

		parser.parse("SELECT * FROM test WHERE A='20'");
		whereClause = parser.getWhereClause();
		assertNotNull(whereClause, "query has a WHERE clause");
		assertEquals("= [A] '20'", whereClause.toString(), "Incorrect WHERE");

		parser.parse("SELECT * FROM test WHERE A='20' AND B='AA'");
		whereClause = parser.getWhereClause();
		assertEquals("AND = [A] '20' = [B] 'AA'",
			whereClause.toString(), "Incorrect WHERE");

		parser.parse("SELECT * FROM test WHERE A='20' OR B='AA'");
		whereClause = parser.getWhereClause();
		assertEquals("OR = [A] '20' = [B] 'AA'",
			whereClause.toString(), "Incorrect WHERE");

		parser.parse("SELECT * FROM test WHERE A='20' OR B='AA' AND c=1");
		whereClause = parser.getWhereClause();
		assertEquals("OR = [A] '20' AND = [B] 'AA' = [C] 1",
			whereClause.toString(), "Incorrect WHERE");

		parser.parse("SELECT * FROM test WHERE (A='20' OR B='AA') AND c=1");
		whereClause = parser.getWhereClause();
		assertEquals("AND OR = [A] '20' = [B] 'AA' = [C] 1",
			whereClause.toString(), "Incorrect WHERE");
	}

	@Test
	public void testWhereMoreParsing() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		String query;

		try
		{
			query = "SELECT * FROM test WHERE FLD_A = '20' AND AND = 'AA'";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		}
		catch (ParseException e)
		{
		}

		try
		{
			query = "SELECT * FROM test WHERE = 'AA'";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		}
		catch (ParseException e)
		{
		}

		try
		{
			query = "SELECT * FROM test WHERE FLD_A = '20' = 'AA'";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		}
		catch (ParseException e)
		{
		}

		parser.parse("SELECT * FROM test WHERE B IS NULL");
		Expression whereClause = parser.getWhereClause();
		assertEquals("N [B]", whereClause.toString(), "Incorrect WHERE");
		parser.parse("SELECT * FROM test WHERE B IS NOT NULL");
		whereClause = parser.getWhereClause();
		assertEquals("NOT N [B]", whereClause.toString(), "Incorrect WHERE");
		parser.parse("SELECT * FROM test WHERE B BETWEEN '20' AND 'AA'");
		whereClause = parser.getWhereClause();
		assertEquals("B [B] '20' 'AA'", whereClause.toString(), "Incorrect WHERE");
		parser.parse("SELECT * FROM test WHERE B NOT BETWEEN '20' AND 'AA'");
		whereClause = parser.getWhereClause();
		assertEquals("NOT B [B] '20' 'AA'", whereClause.toString(), "Incorrect WHERE");
		parser.parse("SELECT * FROM test WHERE B LIKE '20 AND AA'");
		whereClause = parser.getWhereClause();
		parser.parse("SELECT * FROM test WHERE B LIKE '12^_34' ESCAPE '^'");
		whereClause = parser.getWhereClause();
		assertEquals("L [B] '12^_34' ESCAPE '^'", whereClause.toString(), "Incorrect WHERE");
		parser.parse("SELECT * FROM test WHERE B NOT LIKE 'X%'");
		whereClause = parser.getWhereClause();
		assertEquals("NOT L [B] 'X%'", whereClause.toString(), "Incorrect WHERE");
		parser.parse("SELECT * FROM test WHERE B IN ('XX', 'YY')");
		whereClause = parser.getWhereClause();
		assertEquals("IN [B] ('XX', 'YY')", whereClause.toString(), "Incorrect WHERE");

		parser.parse("SELECT * FROM test WHERE B IS NULL OR B BETWEEN '20' AND 'AA' AND B LIKE '20 AND AA'");
		whereClause = parser.getWhereClause();
		assertEquals("OR N [B] AND B [B] '20' 'AA' L [B] '20 AND AA'",
			whereClause.toString(), "Incorrect WHERE");

		try
		{
			query = "SELECT * FROM test WHERE a=0 AND FLD_A";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		}
		catch (ParseException | SQLException e)
		{
		}

		try
		{
			query = "SELECT * FROM test WHERE A * B + 1";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		}
		catch (ParseException | SQLException e)
		{
		}

		try
		{
			query = "SELECT (B > 5) FROM test";
			parser.parse(query);
			fail("incorrect query '" + query + "' parsed as correct");
		}
		catch (ParseException | SQLException e)
		{
		}

		parser.parse("SELECT * FROM test WHERE B = (20)");
		whereClause = parser.getWhereClause();
		assertEquals("= [B] 20", whereClause.toString(), "Incorrect WHERE");
		
		parser.parse("SELECT * FROM test WHERE B = 20 + 30");
		whereClause = parser.getWhereClause();
		assertEquals("= [B] + 20 30", whereClause.toString(), "Incorrect WHERE");
		
		parser.parse("SELECT * FROM test WHERE B + 20 = 30");
		whereClause = parser.getWhereClause();
		assertEquals("= + [B] 20 30", whereClause.toString(), "Incorrect WHERE");

		parser.parse("SELECT * FROM test WHERE (B + 20) = 30");
		whereClause = parser.getWhereClause();
		assertEquals("= + [B] 20 30", whereClause.toString(), "Incorrect WHERE");
	}

	/**
	 * Test that where conditions with AND operator are parsed correctly
	 */
	@Test
	public void testWhereEvaluating() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Map<String, Object> env = new HashMap<>();

		parser.parse("SELECT * FROM test WHERE c=1");
		env.clear();
		env.put("C", Integer.valueOf("1"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));

		parser.parse("SELECT * FROM test WHERE c='1'");
		env.clear();
		env.put("C", new String("1"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));

		parser.parse("SELECT * FROM test WHERE c=1.0");
		env.clear();
		env.put("C", Double.valueOf("1.0"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));

		parser.parse("SELECT * FROM test WHERE (A='20' OR B='AA') AND c=1");
		LogicalExpression whereClause = parser.getWhereClause();

		env.clear();
		env.put("A", new String("20"));
		env.put("B", new String("AA"));
		env.put("C", Integer.valueOf("1"));
		assertEquals(Boolean.TRUE, whereClause.isTrue(env));
		env.put("A", Double.valueOf("20"));
		assertEquals(Boolean.TRUE, whereClause.isTrue(env));
		env.put("B", new String(""));
		assertEquals(Boolean.FALSE, whereClause.isTrue(env));
		env.put("A", new String("20"));
		assertEquals(Boolean.TRUE, whereClause.isTrue(env));
		env.put("C", Integer.valueOf("3"));
		assertEquals(Boolean.FALSE, whereClause.isTrue(env));
	}

	/**
	 * Test that where conditions with AND operator are parsed correctly
	 */
	@Test
	public void testWhereComparisons() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Map<String, Object> env = new HashMap<>();
		env.put("C", Integer.valueOf("12"));

		parser.parse("SELECT * FROM test WHERE c=1");
		assertEquals(Boolean.FALSE, parser.getWhereClause().isTrue(env));
		parser.parse("SELECT * FROM test WHERE c<1");
		assertEquals(Boolean.FALSE, parser.getWhereClause().isTrue(env));
		parser.parse("SELECT * FROM test WHERE c>1");
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
		parser.parse("SELECT * FROM test WHERE c<=1");
		assertEquals(Boolean.FALSE, parser.getWhereClause().isTrue(env));
		parser.parse("SELECT * FROM test WHERE c>=1");
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
		parser.parse("SELECT * FROM test WHERE c<=12");
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
		parser.parse("SELECT * FROM test WHERE c>=12");
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
	}

	@Test
	public void testParsingWhereComparisonsNull() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Map<String, Object> env = new HashMap<>();
		env.put("C", null);

		/*
		 * Test that comparisons containing SQL NULL return null.
		 */
		parser.parse("SELECT * FROM test WHERE c>1");
		assertEquals(null, parser.getWhereClause().isTrue(env));
		parser.parse("SELECT * FROM test WHERE c BETWEEN 1 AND 10");
		assertEquals(null, parser.getWhereClause().isTrue(env));

		parser.parse("SELECT * FROM test WHERE NOT c>1");
		assertEquals(null, parser.getWhereClause().isTrue(env));
		parser.parse("SELECT * FROM test WHERE NOT c BETWEEN 1 AND 10");
		assertEquals(null, parser.getWhereClause().isTrue(env));
	}

	@Test
	public void testParsingWhereEmptyString() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Map<String, Object> env = new HashMap<>();
		env.put("C", new String(""));

		parser.parse("SELECT * FROM test WHERE c=''");
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
	}

	@Test
	public void testParsingWhereSingleQuoteString() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Map<String, Object> env = new HashMap<>();
		env.put("C", new String("it's"));

		parser.parse("SELECT * FROM test WHERE c='it''s'");
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
	}

	@Test
	public void testWhereEvaluatingIndistinguishedNumbers() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Map<String, Object> env = new HashMap<>();

		parser.parse("SELECT * FROM test WHERE c=1.0");
		env.clear();
		env.put("C", Integer.valueOf("1"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
		env.put("C", Double.valueOf("1"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
		env.put("C", Float.valueOf("1"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
	}

	@Test
	public void testWhereEvaluatingIndistinguishedNegativeNumbers() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Map<String, Object> env = new HashMap<>();

		parser.parse("SELECT * FROM test WHERE c=-1.0");
		env.clear();
		env.put("C", Integer.valueOf("-1"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
		env.put("C", Double.valueOf("-1"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
		env.put("C", Float.valueOf("-1"));
		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
	}

	@Test
	public void testParsingQueryEnvironmentEntries() throws ParseException
	{
		ExpressionParser cs;
		cs = new ExpressionParser(new StringReader("*"));
		cs.parseQueryEnvEntry();
		assertEquals("*: *", cs.toString());
		cs = new ExpressionParser(new StringReader("A"));
		cs.parseQueryEnvEntry();
		assertEquals("A: [A]", cs.toString());
		cs = new ExpressionParser(new StringReader("123 A"));
		cs.parseQueryEnvEntry();
		assertEquals("A: 123", cs.toString());
		cs = new ExpressionParser(new StringReader("123+2 A"));
		cs.parseQueryEnvEntry();
		assertEquals("A: + 123 2", cs.toString());
		cs = new ExpressionParser(new StringReader("123/2 A"));
		cs.parseQueryEnvEntry();
		assertEquals("A: / 123 2", cs.toString());
		cs = new ExpressionParser(new StringReader("123/-2 A"));
		cs.parseQueryEnvEntry();
		assertEquals("A: / 123 -2", cs.toString());
		cs = new ExpressionParser(new StringReader("'foo'||'bar' A"));
		cs.parseQueryEnvEntry();
		assertEquals("A: || 'foo' 'bar'", cs.toString());
		cs = new ExpressionParser(new StringReader("'123' A"));
		cs.parseQueryEnvEntry();
		assertEquals("A: '123'", cs.toString());
		cs = new ExpressionParser(new StringReader("A+B AS sum"));
		cs.parseQueryEnvEntry();
		assertEquals("SUM: + [A] [B]", cs.toString());
		cs = new ExpressionParser(new StringReader("A+B sum"));
		cs.parseQueryEnvEntry();
		assertEquals("SUM: + [A] [B]", cs.toString());
		cs = new ExpressionParser(new StringReader("B+C+'123' t1"));
		cs.parseQueryEnvEntry();
		assertEquals("T1: + + [B] [C] '123'", cs.toString());
		cs = new ExpressionParser(new StringReader("loc * par"));
		cs.parseQueryEnvEntry();
		assertEquals("* [LOC] [PAR]: * [LOC] [PAR]", cs.toString());
		cs = new ExpressionParser(new StringReader("123"));
		cs.parseQueryEnvEntry();
		assertEquals("123: 123", cs.toString());
		cs = new ExpressionParser(new StringReader("123+2"));
		cs.parseQueryEnvEntry();
		assertEquals("+ 123 2: + 123 2", cs.toString());
		cs = new ExpressionParser(new StringReader("'123'"));
		cs.parseQueryEnvEntry();
		assertEquals("'123': '123'", cs.toString());
		cs = new ExpressionParser(new StringReader("UPPER(A)"));
		cs.parseQueryEnvEntry();
		assertEquals("UPPER([A]): UPPER([A])", cs.toString());
		cs = new ExpressionParser(new StringReader("LENGTH(A)"));
		cs.parseQueryEnvEntry();
		assertEquals("LENGTH([A]): LENGTH([A])", cs.toString());
		cs = new ExpressionParser(new StringReader("TRIM(A)"));
		cs.parseQueryEnvEntry();
		assertEquals("TRIM([A]): TRIM([A])", cs.toString());
	}

	@Test
	public void testParsingQueryEnvironmentWithoutExpressions()
			throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		parser.parse("SELECT A FROM test");
		assertEquals("[A]", parser.getExpression(0).toString());
		parser.parse("SELECT 123 a FROM test");
		assertEquals("123", parser.getExpression(0).toString());
		parser.parse("SELECT '123' a FROM test");
		assertEquals("'123'", parser.getExpression(0).toString());
	}

	@Test
	public void testParsingQueryEnvironmentWithExpressions() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		parser.parse("SELECT A+B AS SUM FROM test");
		assertEquals("+ [A] [B]", parser.getExpression(0).toString());
		parser.parse("SELECT A+B SUM FROM test");
		assertEquals("+ [A] [B]", parser.getExpression(0).toString());
		parser.parse("SELECT A+B SUM, B+C+'123' t12 FROM test");
		assertEquals("+ [A] [B]", parser.getExpression(0).toString());
		assertEquals("+ + [B] [C] '123'", parser.getExpression(1).toString());
	}

	@Test
	public void testEvaluateBinaryOperationsSum() throws ParseException, SQLException
	{
		ExpressionParser cs;
		cs = new ExpressionParser(new StringReader("A+b AS result"));
		cs.parseQueryEnvEntry();
		Map<String, Object> env = new HashMap<>();
		env.put("A", Integer.valueOf(1));

		env.put("B", Integer.valueOf(1));
		assertEquals(Integer.valueOf("2"), cs.eval(env));
		env.put("A", Double.valueOf(1));
		assertEquals(Double.valueOf("2"), cs.eval(env));
		env.put("A", new String("1"));
		// string concatenation because one of the arguments is a string
		assertEquals("11", ""+cs.eval(env));
	}


	@Test
	public void testEvaluateBinaryOperationsConcat() throws ParseException, SQLException
	{
		ExpressionParser cs;
		cs = new ExpressionParser(new StringReader("A || B AS result"));
		cs.parseQueryEnvEntry();
		Map<String, Object> env = new HashMap<>();
		env.put("A", "Hello");
		env.put("B", "World");
		assertEquals("HelloWorld", cs.eval(env));
		env.put("B", Integer.valueOf(100));
		assertEquals("Hello100", cs.eval(env));
	}

	@Test
	public void testEvaluateBinaryOperationsModulo() throws ParseException, SQLException
	{
		ExpressionParser cs;

		cs = new ExpressionParser(new StringReader("A%b AS result"));
		cs.parseQueryEnvEntry();

		Map<String, Object> env = new HashMap<>();

		env.put("A", Integer.valueOf(4));
		env.put("B", Integer.valueOf(3));
		assertEquals(Integer.valueOf("1"), cs.eval(env));

		env.put("A", Integer.valueOf(-3));
		env.put("B", Integer.valueOf(2));
		assertEquals(Integer.valueOf("-1"), cs.eval(env));

		env.put("A", Integer.valueOf(4));
		env.put("B", Integer.valueOf(0));
		try
		{
			cs.eval(env);
			fail("Should raise a java.sql.SQLException");
		}
		catch (SQLException e)
		{
			
		}

		env.put("A", Double.valueOf(5));
		env.put("B", Double.valueOf(3));
		assertEquals(Double.valueOf("2"), cs.eval(env));

		env.put("A", Double.valueOf(8.8));
		env.put("B", Double.valueOf(3.3));
		assertEquals(Double.valueOf("2.2"), cs.eval(env));

		env.put("A", Double.valueOf(-5));
		env.put("B", Double.valueOf(3));
		assertEquals(Double.valueOf("-2"), cs.eval(env));
	}

	@Test
	public void testEvaluateBinaryOperationsOtherThanSum() throws ParseException, SQLException
	{
		ExpressionParser cs;
		cs = new ExpressionParser(new StringReader("a-b AS result"));
		cs.parseQueryEnvEntry();
		Map<String, Object> env = new HashMap<>();
		env.put("A", Integer.valueOf(5));

		env.put("B", Integer.valueOf(1));
		assertEquals(Integer.valueOf("4"), cs.eval(env));
		env.put("B", Double.valueOf(1));
		assertEquals(Double.valueOf("4"), cs.eval(env));

		cs = new ExpressionParser(new StringReader("a*b AS result"));
		cs.parseQueryEnvEntry();

		env.put("B", Integer.valueOf(1));
		assertEquals(Integer.valueOf("5"), cs.eval(env));
		env.put("B", Double.valueOf(1));
		assertEquals(Double.valueOf("5"), cs.eval(env));
		
		cs = new ExpressionParser(new StringReader("a/b AS result"));
		cs.parseQueryEnvEntry();
		env.put("B", Integer.valueOf(2));
		assertEquals(Integer.valueOf("2"), cs.eval(env));
		env.put("B", Double.valueOf(2));
		assertEquals(Double.valueOf("2.5"), cs.eval(env));

		env.put("B", Double.valueOf(0));
		try
		{
			cs.eval(env);
			fail("Should raise a java.sql.SQLException");
		}
		catch (SQLException e)
		{
		}
	}

	@Test
	public void testEvaluateShortOperations() throws ParseException, SQLException
	{
		ExpressionParser cs;
		cs = new ExpressionParser(new StringReader("A+1 AS result"));
		cs.parseQueryEnvEntry();
		Map<String, Object> env = new HashMap<>();
		env.put("A", Short.valueOf("1"));
		Object o = cs.eval(env);
		assertEquals(o.toString(), "2");

		cs = new ExpressionParser(new StringReader("A+B AS result"));
		cs.parseQueryEnvEntry();		
		env.put("A", Short.valueOf("3"));
		env.put("B", Short.valueOf("4"));
		o = cs.eval(env);
		assertEquals(o.toString(), "7");
		
		cs = new ExpressionParser(new StringReader("A/2 AS result"));
		cs.parseQueryEnvEntry();		
		env.put("A", Short.valueOf("25"));
		o = cs.eval(env);
		assertEquals(o.toString(), "12");
	}

	@Test
	public void testEvaluateLongOperations() throws ParseException, SQLException
	{
		ExpressionParser cs;
		cs = new ExpressionParser(new StringReader("A+5678678678 AS result"));
		cs.parseQueryEnvEntry();
		Map<String, Object> env = new HashMap<>();
		env.put("A", Integer.valueOf("1"));
		Object o = cs.eval(env);
		assertEquals(o.toString(), "5678678679");

		cs = new ExpressionParser(new StringReader("A-50000000000 AS result"));
		cs.parseQueryEnvEntry();		
		env.put("A", Long.valueOf("120000000000"));
		o = cs.eval(env);
		assertEquals(o.toString(), "70000000000");

		cs = new ExpressionParser(new StringReader("A*5000000000 AS result"));
		cs.parseQueryEnvEntry();		
		env.put("A", Integer.valueOf("3"));
		o = cs.eval(env);
		assertEquals(o.toString(), "15000000000");

		cs = new ExpressionParser(new StringReader("A*10000L AS result"));
		cs.parseQueryEnvEntry();		
		env.put("A", Integer.valueOf("1000000"));
		o = cs.eval(env);
		assertEquals(o.toString(), "10000000000");

		cs = new ExpressionParser(new StringReader("A/10 AS result"));
		cs.parseQueryEnvEntry();		
		env.put("A", Long.valueOf("-1234567891230"));
		o = cs.eval(env);
		assertEquals(o.toString(), "-123456789123");
	}

	@Test
	public void testEvaluateDateOperations() throws ParseException, SQLException
	{
		ExpressionParser cs;
		cs = new ExpressionParser(new StringReader("CURRENT_DATE AS now"));
		cs.parseQueryEnvEntry();
		Map<String, Object> env = new HashMap<>();
		env.put(StringConverter.COLUMN_NAME, new StringConverter("yyyy-mm-dd", "HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "UTC", false));

		// Protect against unlikely situation of test running over date change at midnight.
		String date1 = new Date(System.currentTimeMillis()).toString();
		Object o = cs.eval(env);
		String date2 = new Date(System.currentTimeMillis()).toString();
		assertTrue(date1.equals(o.toString()) || date2.equals(o.toString()));

		cs = new ExpressionParser(new StringReader("EXPIRYDATE + 10 as result"));
		cs.parseQueryEnvEntry();
		env.put("EXPIRYDATE", Date.valueOf("2011-11-24"));
		o = cs.eval(env);
		assertEquals(o.toString(), "2011-12-04");

		cs = new ExpressionParser(new StringReader("10 + EXPIRYDATE as result"));
		cs.parseQueryEnvEntry();
		env.put("EXPIRYDATE", Date.valueOf("2011-11-24"));
		o = cs.eval(env);
		assertEquals(o.toString(), "2011-12-04");

		cs = new ExpressionParser(new StringReader("EXPIRYDATE - 10 as result"));
		cs.parseQueryEnvEntry();
		env.put("EXPIRYDATE", Date.valueOf("2011-11-24"));
		o = cs.eval(env);
		assertEquals(o.toString(), "2011-11-14");

		cs = new ExpressionParser(new StringReader("EXPIRYDATE - '2011-11-01' as result"));
		cs.parseQueryEnvEntry();
		env.put("EXPIRYDATE", Date.valueOf("2011-11-24"));
		o = cs.eval(env);
		assertEquals(o, Integer.valueOf(23));

		cs = new ExpressionParser(new StringReader("ENDDATE - STARTDATE + 1 as result"));
		cs.parseQueryEnvEntry();
		env.put("STARTDATE", Date.valueOf("2011-11-22"));
		env.put("ENDDATE", Date.valueOf("2011-11-24"));
		o = cs.eval(env);
		assertEquals(o, Integer.valueOf(3));
	}

	@Test
	public void testEvaluateTimeOperations() throws ParseException, SQLException
	{
		ExpressionParser cs;
		cs = new ExpressionParser(new StringReader("CURRENT_TIME AS T1"));
		cs.parseQueryEnvEntry();
		Map<String, Object> env = new HashMap<>();
		Time t1 = (Time)cs.eval(env);
		assertNotNull(t1);
		// Avoid comparison with current time because it is changing as unit test runs 
	}

	@Test
	public void testEvaluateCaseExpressions() throws ParseException, SQLException
	{
		/*
		 * Test "searched case" expressions.
		 */
		ExpressionParser cs; 
		cs = new ExpressionParser(new StringReader("CASE " +
			"WHEN POSTCODE >= 2600 AND POSTCODE <= 2618 THEN 'ACT' " +
			"WHEN POSTCODE >= 2000 AND POSTCODE <= 2999 THEN 'NSW' " +
			"WHEN POSTCODE >= 3000 AND POSTCODE <= 3999 THEN 'VIC' " +
			"ELSE '' END"));
		cs.parseQueryEnvEntry();
		Map<String, Object> env = new HashMap<>();

		env.put("POSTCODE", Integer.valueOf(2601));
		Object o = cs.eval(env);
		assertEquals(o.toString(), "ACT");

		env.put("POSTCODE", Integer.valueOf(2795));
		o = cs.eval(env);
		assertEquals(o.toString(), "NSW");

		env.put("POSTCODE", Integer.valueOf(3001));
		o = cs.eval(env);
		assertEquals(o.toString(), "VIC");

		env.put("POSTCODE", Integer.valueOf(6000));
		o = cs.eval(env);
		assertEquals(o.toString(), "");

		cs = new ExpressionParser(new StringReader("CASE WHEN F=1 THEN '1st' WHEN F=2 THEN '2nd' END"));
		cs.parseQueryEnvEntry();

		env.put("F", Integer.valueOf(1));
		o = cs.eval(env);
		assertEquals(o.toString(), "1st");

		env.put("F", Integer.valueOf(2));
		o = cs.eval(env);
		assertEquals(o.toString(), "2nd");

		env.put("F", Integer.valueOf(3));
		o = cs.eval(env);
		assertEquals(o, null);

		/*
		 * Test "simple case" expressions.
		 */
		cs = new ExpressionParser(new StringReader("CASE UNITS WHEN 'KM' THEN X * 1000 ELSE X END"));
		cs.parseQueryEnvEntry();

		env.put("UNITS", "KM");
		env.put("X", Integer.valueOf(3));
		o = cs.eval(env);
		assertEquals(o.toString(), "3000");

		env.put("UNITS", "M");
		o = cs.eval(env);
		assertEquals(o.toString(), "3");

		cs = new ExpressionParser(new StringReader("CASE F WHEN 1 THEN '1st' WHEN 2 THEN '2nd' END"));
		cs.parseQueryEnvEntry();

		env.put("F", Integer.valueOf(1));
		o = cs.eval(env);
		assertEquals(o.toString(), "1st");

		env.put("F", Integer.valueOf(2));
		o = cs.eval(env);
		assertEquals(o.toString(), "2nd");

		env.put("F", Integer.valueOf(3));
		o = cs.eval(env);
		assertEquals(o, null);
	}

	@Test
	public void testParsingIgnoresCase() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		parser.parse("SELECT A+B AS SUM FROM test");
		assertEquals("+ [A] [B]", parser.getExpression(0).toString());
		parser.parse("SELECT A+B As SUM FROM test");
		assertEquals("+ [A] [B]", parser.getExpression(0).toString());
		parser.parse("SELECT A+B aS SUM FROM test");
		assertEquals("+ [A] [B]", parser.getExpression(0).toString());
		parser.parse("SELECT A+B as SUM FROM test");
		assertEquals("+ [A] [B]", parser.getExpression(0).toString());
	}

	@Test
	public void testParsingTableAlias() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("SELECT Ab.Name FROM sample AS Ab WHERE Ab.ID='A123'");
		assertEquals("AB", parser.getTableAliases().get(0));
	}

	@Test
	public void testParsingWithNewlines() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("\r\nSELECT NAME\r\nas FLD_A\r\nFROM test\r\nWHERE ID=1\r\n");
		assertTrue(parser.getTableNames().get(0).equals("test"), "Incorrect table name");

		String[] cols = parser.getColumnNames();
		assertTrue(cols.length == 1, "Incorrect Column Count");

		assertTrue(cols[0].equalsIgnoreCase("fld_a"),
			"Column Name Col 0 '" + cols[0] + "' is not equal FLD_A");
	}

	@Test
	public void testParsingComma() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("SELECT Id + ',' + Name FROM sample");
		assertEquals("+ + [ID] ',' [NAME]", parser.getExpression(0).toString());
	}

	@Test
	public void testParsingQuotedFrom() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Expression whereClause;

		parser.parse("SELECT Id FROM sample where Signature = 'sent from my iPhone'");
		whereClause = parser.getWhereClause();
		assertNotNull(whereClause, "query has a WHERE clause");
		assertEquals("= [SIGNATURE] 'sent from my iPhone'", whereClause.toString(), "Incorrect WHERE");
	}

	@Test
	public void testParsingQuotedWhere() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();
		Expression whereClause;

		parser.parse("SELECT Id FROM sample where Title like '%NOT WHERE I BELONG%'");
		whereClause = parser.getWhereClause();
		assertNotNull(whereClause, "query has a WHERE clause");
		assertEquals("L [TITLE] '%NOT WHERE I BELONG%'", whereClause.toString(), "Incorrect WHERE");
	}

	@Test
	public void testParsingEndingWithSemiColon() throws ParseException, SQLException
	{
		SqlParser parser1 = new SqlParser();
		parser1.parse("SELECT Id FROM sample;");
		assertEquals("sample", parser1.getTableNames().get(0));

		SqlParser parser2 = new SqlParser();
		parser2.parse("SELECT Id FROM sample\n;");
		assertEquals("sample", parser2.getTableNames().get(0));
	}

	@Test
	public void testParsingComments() throws ParseException, SQLException
	{
		SqlParser parser1 = new SqlParser();
		parser1.parse("-- Comment Before\n" +
			"--\n" +
			"SELECT Id FROM sample\n" +
			"-- Comment After");
		assertEquals("sample", parser1.getTableNames().get(0));
		String[] cols = parser1.getColumnNames();
		assertTrue(cols.length == 1, "Incorrect Column Count");
		assertTrue(cols[0].equalsIgnoreCase("Id"),
			"Column Name Col 0 '" + cols[0] + "' is not equal Id");

		SqlParser parser2 = new SqlParser();
		parser2.parse("SELECT\r\n" +
			"MONTH, -- 1=January\r\n" +
			"TEMPERATURE * (9.0 / 5) + 32 AS TEMP -- in Fahrenheit\r\n" +
			"FROM\r\n" +
			"climate\r\n" +
			"WHERE\r\n" +
			"CITYID = 77 -- See CITYINDEX.TXT\r\n");
		assertEquals("climate", parser2.getTableNames().get(0));
		cols = parser2.getColumnNames();
		assertTrue(cols.length == 2, "Incorrect Column Count");
		assertTrue(cols[0].equalsIgnoreCase("MONTH"),
			"Column Name Col 0 '" + cols[0] + "' is not equal MONTH");
		assertTrue(cols[1].equalsIgnoreCase("TEMP"),
			"Column Name Col 1 '" + cols[1] + "' is not equal TEMP");
	}

	@Test
	public void testParsingCComments() throws ParseException, SQLException
	{
		SqlParser parser1 = new SqlParser();
		parser1.parse("/*\n" +
			" * A multi-line comment !\n" +
			" */\n" +
			"SELECT Name /* comment */, 'abc/*def' S\n" +
			"FROM sample\n" +
			"/* another comment */");
		assertEquals("sample", parser1.getTableNames().get(0));
		String[] cols = parser1.getColumnNames();
		assertTrue(cols.length == 2, "Incorrect Column Count");
		assertTrue(cols[0].equalsIgnoreCase("NAME"),
			"Column Name Col 0 '" + cols[0] + "' is not NAME");
		assertTrue(cols[1].equalsIgnoreCase("S"),
			"Column Name Col 1 '" + cols[1] + "' is not S");

		try
		{
			SqlParser parser2 = new SqlParser();
			parser2.parse("SELECT /* nested /* comment */ not allowed */ Name FROM sample");
			fail("Should raise a ParseException");
		}
		catch (SQLException | ParseException e)
		{
		}
	}

	@Test
	public void testParsingMultipleStatements() throws ParseException, SQLException
	{
		MultipleSqlParser parser = new MultipleSqlParser();
		List<SqlParser> parsers = parser.parse("SELECT A FROM test1 ; SELECT B FROM test2 ; SELECT C FROM test3");
		assertEquals(3, parsers.size(), "SQL statement count");
		
		assertEquals("[A]", parsers.get(0).getExpression(0).toString());
		assertEquals(parsers.get(0).getTableNames().get(0), "test1", "Incorrect table name");

		assertEquals("[B]", parsers.get(1).getExpression(0).toString());
		assertEquals(parsers.get(1).getTableNames().get(0), "test2", "Incorrect table name");

		assertEquals("[C]", parsers.get(2).getExpression(0).toString());
		assertEquals(parsers.get(2).getTableNames().get(0), "test3", "Incorrect table name");
	}
	
	@Test
	public void testParsingSubquery() throws ParseException, SQLException
	{
		SqlParser parser = new SqlParser();

		parser.parse("SELECT ID, (SELECT SUM(Amount) FROM t2 WHERE sample.ID=t2.ID) FROM sample");
		String subquery = parser.getExpression(1).toString();
		assertTrue(subquery.startsWith("(SELECT"), "Incorrect Subquery");
		assertTrue(subquery.endsWith(")"), "Incorrect Subquery");
	}

	@Test
	public void testWhereDiacritics() throws SQLException, ParseException
	{
		SqlParser parser = new SqlParser();
		Map<String, Object> env = new HashMap<>();
		env.put("C", new String(
				"\u011B\u0161\u010D\u0159\u017E\u00FD\u00E1\u00ED\u00E9\u00FA\u016F\u010F\u0165\u0148\u011A\u0160\u010C\u0158\u017D\u00DD\u00C1\u00CD\u00C9\u00DA\u016E\u010E\u0164\u0147"));

		/*
		 *  Test with Unicode characters with code points greater than 255.
		 *  \u0165 is LATIN SMALL LETTER T WITH CARON
		 *  \u0148 is LATIN SMALL LETTER N WITH CARON
		 *  \u011A is LATIN CAPITAL LETTER E WITH CARON
		 */
		parser.parse("SELECT * FROM test WHERE c LIKE '%\u0165\u0148\u011A%'");

		assertEquals(Boolean.TRUE, parser.getWhereClause().isTrue(env));
	}
}
