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
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/

package org.relique.jdbc.csv;

import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.BeforeClass;
import org.junit.Test;
import org.relique.jdbc.csv.StringConverter;

/**
 * This class is used to test the SqlParser class.
 * 
 * @author Mario Frasca
 */
public class TestStringConverter
{	
	private static DateFormat toUTC;

	@BeforeClass
	public static void setUp()
	{
		toUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		toUTC.setTimeZone(TimeZone.getTimeZone("UTC"));  
	}

	@Test
	public void testParseDateFixedSize()
	{
		StringConverter sc = new StringConverter("dd-mm-yyyy", "", "", "");
		
		Date got, expect;

		got = sc.parseDate("01-01-1980");
		expect = java.sql.Date.valueOf("1980-01-01");
		assertEquals(expect, got);
		
		got = sc.parseDate("3-3-1983");
		expect = null;
		assertEquals(expect, got);
	}

	@Test
	public void testParseDateVariableSize()
	{
		StringConverter sc = new StringConverter("m-d-yyyy", "", "", "");

		Date got, expect;

		got = sc.parseDate("01-01-1980");
		expect = java.sql.Date.valueOf("1980-01-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("3-3-1983");
		expect = java.sql.Date.valueOf("1983-03-03");
		assertEquals(got, expect);
	}

	@Test
	public void testParseDateVariableSizeYMD()
	{
		StringConverter sc = new StringConverter("yyyy-m-d", "", "", "");

		Date got, expect;

		got = sc.parseDate("1980-12-01");
		expect = java.sql.Date.valueOf("1980-12-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("1983-3-9");
		expect = java.sql.Date.valueOf("1983-03-09");
		assertEquals(got, expect);
	}

	@Test
	public void testParseDateVariableSizeMYD()
	{
		StringConverter sc = new StringConverter("m-yyyy-d", "", "", "");

		Date got, expect;

		got = sc.parseDate("12-1980-01");
		expect = java.sql.Date.valueOf("1980-12-01");
		assertEquals(got, expect);
		
		got = sc.parseDate("3-1983-9");
		expect = java.sql.Date.valueOf("1983-03-09");
		assertEquals(got, expect);
	}

	@Test
	public void testParseDateNamedMonth()
	{
		StringConverter sc = new StringConverter("dd-MMM-yyyy", "", "", "", Locale.ENGLISH);

		Date got, expect;

		got = sc.parseDate("21-APR-2014");
		expect = java.sql.Date.valueOf("2014-04-21");
		assertEquals(got, expect);
		
		got = sc.parseDate("21-April-2014");
		expect = java.sql.Date.valueOf("2014-04-21");
		assertEquals(got, expect);
	}
	
	@Test
	public void testParseDateNamedDay()
	{
		StringConverter sc = new StringConverter("EEE, MMM d, yyyy", "", "", "", Locale.ENGLISH);

		Date got, expect;

		got = sc.parseDate("Mon, Apr 21, 2014");
		expect = java.sql.Date.valueOf("2014-04-21");
		assertEquals(got, expect);
		
		got = sc.parseDate("Monday, April 21, 2014");
		expect = java.sql.Date.valueOf("2014-04-21");
		assertEquals(got, expect);
	}

	@Test
	public void testParseTimeMilliseconds()
	{
		StringConverter sc = new StringConverter("", "HH:mm:ss.SSS", "", "", Locale.ENGLISH);

		Time got, expect;

		// Ensure milliseconds component is included in time.
		got = sc.parseTime("14:21:07.858");
		expect = java.sql.Time.valueOf("14:21:07");
		expect = new Time(expect.getTime() + 858);
		assertEquals(got, expect);
	}
	
	@Test
	public void testParseTimestampWithTimeZoneGuadeloupe()
	{
		// Guadeloupe lies 4 hours behind UTC, no daylight savings
		StringConverter sc = new StringConverter("", "", "yyyy-MM-dd HH:mm:ss", "America/Guadeloupe");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 16:00:00", toUTC.format(got));

		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 16:00:00", toUTC.format(got));
	}

	@Test
	public void testParseDateWithTimeZoneYakutsk()
	{
		// in January Yakutsk lies 9 hours ahead of UTC
		StringConverter sc = new StringConverter("", "", "yyyy-MM-dd HH:mm:ss", "Asia/Yakutsk");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 03:00:00", toUTC.format(got));

		// in July Yakutsk lies 10 hours ahead of UTC
		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 02:00:00", toUTC.format(got));
	}

	@Test
	public void testParseDateWithTimeZoneSantiago()
	{
		// in January Santiago lies 3 hours behind of UTC
		StringConverter sc = new StringConverter("", "", "yyyy-MM-dd HH:mm:ss", "America/Santiago");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 15:00:00", toUTC.format(got));

		// in July Santiago lies 4 hours behind UTC
		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 16:00:00", toUTC.format(got));
	}

	@Test
	public void testParseDateWithTimeZoneAthens()
	{
		// in January Athens lies 2 hours ahead of UTC
		StringConverter sc = new StringConverter("", "", "yyyy-MM-dd HH:mm:ss", "Europe/Athens");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 10:00:00", toUTC.format(got));

		// in July Athens lies 3 hours ahead of UTC
		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 09:00:00", toUTC.format(got));
	}

	@Test
	public void testParseDateWithTimeZoneDefaultJanuary()
	{
		// defaulting to UTC
		StringConverter sc = new StringConverter("", "", "yyyy-MM-dd HH:mm:ss", "");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 12:00:00", toUTC.format(got));
	}

	@Test
	public void testParseDateWithTimeZoneDefaultJuly()
	{
		// defaulting to UTC
		StringConverter sc = new StringConverter("", "", "yyyy-MM-dd HH:mm:ss", "");
		Timestamp got;

		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 12:00:00", toUTC.format(got));
	}

	@Test
	public void testParseDateWithTimeZoneUTCJanuary()
	{
		// explicit UTC
		StringConverter sc = new StringConverter("", "", "yyyy-MM-dd HH:mm:ss", "UTC");
		Timestamp got;

		got = sc.parseTimestamp("2010-01-01 12:00:00");
		assertEquals("2010-01-01 12:00:00", toUTC.format(got));
	}

	@Test
	public void testParseDateWithTimeZoneUTCJuly()
	{
		// explicit UTC
		StringConverter sc = new StringConverter("", "", "yyyy-MM-dd HH:mm:ss", "UTC");
		Timestamp got;

		got = sc.parseTimestamp("2010-07-01 12:00:00");
		assertEquals("2010-07-01 12:00:00", toUTC.format(got));
	}
	
	@Test
	public void testParseTimestampWithFormat()
	{
		// explicit UTC
		StringConverter sc = new StringConverter("", "", "dd-MMM-yy hh.mm.ss.000000 aa", "UTC");
		Timestamp got;

		got = sc.parseTimestamp("25-NOV-13 01.29.07.000000 PM");
		assertEquals("2013-11-25 13:29:07", toUTC.format(got));
	}
}
