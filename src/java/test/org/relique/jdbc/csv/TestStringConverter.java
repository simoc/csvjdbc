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

}
