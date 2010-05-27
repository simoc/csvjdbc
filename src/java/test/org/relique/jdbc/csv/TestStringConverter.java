/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2009 Mario Frasca

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

import java.sql.Date;

import org.relique.jdbc.csv.StringConverter;

import junit.framework.TestCase;

/**
 * This class is used to test the SqlParser class.
 * 
 * @author Mario Frasca
 * @version $Id: TestStringConverter.java,v 1.3 2010/05/27 12:00:09 mfrasca Exp $
 */
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
