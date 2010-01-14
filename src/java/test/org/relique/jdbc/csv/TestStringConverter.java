package test.org.relique.jdbc.csv;

import java.sql.Date;

import org.relique.jdbc.csv.StringConverter;

import junit.framework.TestCase;

public class TestStringConverter extends TestCase {

	public void testParseDateFixedSize() {
		StringConverter sc = new StringConverter("dd-mm-yyyy","");
		
		Date got, expect;

		got = sc.parseDate("01-01-1980");
		expect = java.sql.Date.valueOf("1980-01-01");
		assertEquals(expect, got);
		
		got = sc.parseDate("3-3-1983");
		expect = java.sql.Date.valueOf("1970-01-01");
		assertEquals(expect, got);
	}

	public void testParseDateVariableSize() {
		StringConverter sc = new StringConverter("m-d-yyyy","");

		Date got, expect;

		got = sc.parseDate("01-01-1980");
		expect = java.sql.Date.valueOf("1980-01-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("3-3-1983");
		expect = java.sql.Date.valueOf("1983-03-03");
		assertEquals(got, expect);
	}

	public void testParseDateVariableSizeYMD() {
		StringConverter sc = new StringConverter("yyyy-m-d","");

		Date got, expect;

		got = sc.parseDate("1980-12-01");
		expect = java.sql.Date.valueOf("1980-12-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("1983-3-9");
		expect = java.sql.Date.valueOf("1983-03-09");
		assertEquals(got, expect);
	}

	public void testParseDateVariableSizeMYD() {
		StringConverter sc = new StringConverter("m-yyyy-d","");

		Date got, expect;

		got = sc.parseDate("12-1980-01");
		expect = java.sql.Date.valueOf("1980-12-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("3-1983-9");
		expect = java.sql.Date.valueOf("1983-03-09");
		assertEquals(got, expect);
	}

}
