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
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.relique.jdbc.csv.StringConverter;

import junit.framework.TestCase;

/**
 * This class is used to test the SqlParser class.
 * 
 * @author Mario Frasca
 * @version $Id: TestStringConverter.java,v 1.4 2010/09/14 15:03:09 mfrasca Exp $
 */
public class TestStringConverter extends TestCase {
	
	private DateFormat toUTC;

	public void setUp() {
		toUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		toUTC.setTimeZone(TimeZone.getTimeZone("UTC"));  
	}
	
	public static String timestampToUTC(Timestamp ts) {
		TestStringConverter o = new TestStringConverter();
		o.setUp();
		return o.toUTC.format(ts);
	}

	public void testParseDateFixedSize() {
		StringConverter sc = new StringConverter("dd-mm-yyyy", "", "");
		
		Date got, expect;

		got = sc.parseDate("01-01-1980");
		expect = java.sql.Date.valueOf("1980-01-01");
		assertEquals(expect, got);
		
		got = sc.parseDate("3-3-1983");
		expect = java.sql.Date.valueOf("1970-01-01");
		assertEquals(expect, got);
	}

	public void testParseDateVariableSize() {
		StringConverter sc = new StringConverter("m-d-yyyy", "", "");

		Date got, expect;

		got = sc.parseDate("01-01-1980");
		expect = java.sql.Date.valueOf("1980-01-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("3-3-1983");
		expect = java.sql.Date.valueOf("1983-03-03");
		assertEquals(got, expect);
	}

	public void testParseDateVariableSizeYMD() {
		StringConverter sc = new StringConverter("yyyy-m-d", "", "");

		Date got, expect;

		got = sc.parseDate("1980-12-01");
		expect = java.sql.Date.valueOf("1980-12-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("1983-3-9");
		expect = java.sql.Date.valueOf("1983-03-09");
		assertEquals(got, expect);
	}

	public void testParseDateVariableSizeMYD() {
		StringConverter sc = new StringConverter("m-yyyy-d", "", "");

		Date got, expect;

		got = sc.parseDate("12-1980-01");
		expect = java.sql.Date.valueOf("1980-12-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("3-1983-9");
		expect = java.sql.Date.valueOf("1983-03-09");
		assertEquals(got, expect);
	}

	public void testParseTimestampWithTimeZoneGuadeloupe() {
		// Guadeloupe lies 4 hours behind UTC, no daylight savings
		StringConverter sc = new StringConverter("", "", "America/Guadeloupe");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 16:00:00", toUTC.format(got));

		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 16:00:00", toUTC.format(got));
	}
	
	public void testParseDateWithTimeZoneYakutsk() {
		// in January Yakutsk lies 9 hours ahead of UTC
		StringConverter sc = new StringConverter("", "", "Asia/Yakutsk");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 03:00:00", toUTC.format(got));

		// in July Yakutsk lies 10 hours ahead of UTC
		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 02:00:00", toUTC.format(got));
	}

	public void testParseDateWithTimeZoneSantiago() {
		// in January Santiago lies 3 hours behind of UTC
		StringConverter sc = new StringConverter("", "", "America/Santiago");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 15:00:00", toUTC.format(got));

		// in July Santiago lies 4 hours behind UTC
		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 16:00:00", toUTC.format(got));
	}

	public void testParseDateWithTimeZoneAthens() {
		// in January Athens lies 2 hours ahead of UTC
		StringConverter sc = new StringConverter("", "", "Europe/Athens");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 10:00:00", toUTC.format(got));

		// in July Athens lies 3 hours ahead of UTC
		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 09:00:00", toUTC.format(got));
	}

	public void testParseDateWithTimeZoneDefaultJanuary() {
		// defaulting to UTC
		StringConverter sc = new StringConverter("", "", "");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 12:00:00", toUTC.format(got));
	}

	public void testParseDateWithTimeZoneDefaultJuly() {
		// defaulting to UTC
		StringConverter sc = new StringConverter("", "", "");
		Timestamp got;

		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 12:00:00", toUTC.format(got));
	}

	public void testParseDateWithTimeZoneUTCJanuary() {
		// explicit UTC
		StringConverter sc = new StringConverter("", "", "UTC");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 12:00:00", toUTC.format(got));
	}

	public void testParseDateWithTimeZoneUTCJuly() {
		// explicit UTC
		StringConverter sc = new StringConverter("", "", "UTC");
		Timestamp got;

		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 12:00:00", toUTC.format(got));
	}

}
