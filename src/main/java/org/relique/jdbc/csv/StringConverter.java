/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringConverter
{
	private String dateFormat;
	private SimpleDateFormat simpleTimeFormat;
	private String timeFormat;
	private GregorianCalendar calendar;
	private Pattern timestampPattern;
	private SimpleDateFormat timestampFormat;
	private SimpleDateFormat simpleDateFormat;

	public StringConverter(String dateformat, String timeformat, String timestampformat,
		String timeZoneName)
	{
		init(dateformat, timeformat, timestampformat, timeZoneName, null);
	}

	public StringConverter(String dateformat, String timeformat, String timestampformat,
		String timeZoneName, Locale locale)
	{
		init(dateformat, timeformat, timestampformat, timeZoneName, locale);
	}

	private void init(String dateformat, String timeformat, String timestampformat,
		String timeZoneName, Locale locale)
	{
		dateFormat = dateformat;
		if (dateformat != null)
		{
			/*
			 * Can date be parsed with a simple regular expression, or is the full
			 * SimpleDateFormat parsing required?
			 */
			// TODO prefer to use SimpleDateFormat for everything but existing regex not 100% compatible
			String upper = dateformat.toUpperCase();
			boolean useSimpleDateFormat = false;
			if (upper.contains("MMM"))
			{
				/*
				 * Dates contain named months -- we need to use a SimpleDateFormat to parse them.
				 */
				useSimpleDateFormat = true;
			}
			else
			{
				for (int i = 0; i < upper.length(); i++)
				{
					char c = upper.charAt(i);
					if (Character.isLetter(c) && c != 'D' && c != 'M' && c != 'Y')
					{
						/*
						 * Dates are not just a straightforward format with days, months,
						 * years -- we need to use a SimpleDateFormat to parse them.
						 */
						useSimpleDateFormat = true;
					}
				}
			}
			if (useSimpleDateFormat)
			{
				/*
				 * Use Java API for parsing dates.
				 */
				if (locale != null)
				{
					DateFormatSymbols symbols = DateFormatSymbols.getInstance(locale);
					simpleDateFormat = new SimpleDateFormat(dateformat, symbols);
				}
				else
				{
					simpleDateFormat = new SimpleDateFormat(dateformat);
				}
			}
		}
		
		/*
		 * Use Java API for parsing times.
		 */
		timeFormat = timeformat;
		if (locale != null)
		{
			DateFormatSymbols symbols = DateFormatSymbols.getInstance(locale);
			simpleTimeFormat = new SimpleDateFormat(timeformat, symbols);
		}
		else
		{
			simpleTimeFormat = new SimpleDateFormat(timeformat);
		}

		TimeZone timeZone = TimeZone.getTimeZone(timeZoneName);
		calendar = new GregorianCalendar();
		calendar.clear();
		calendar.setTimeZone(timeZone);
		if (timestampformat != null && timestampformat.length() > 0)
		{
			/*
			 * Use Java API for parsing dates and times.
			 */
			if (locale != null)
			{
				DateFormatSymbols symbols = DateFormatSymbols.getInstance(locale);
				timestampFormat = new SimpleDateFormat(timestampformat, symbols);
			}
			else
			{
				timestampFormat = new SimpleDateFormat(timestampformat);
			}
			timestampFormat.setTimeZone(timeZone);
		}
		else
		{
			/*
			 * Parse timestamps using a fixed regular expression.
			 */
			timestampPattern = Pattern
				.compile("([0-9][0-9][0-9][0-9])-([0-9]?[0-9])-([0-9]?[0-9])[ T]([0-9]?[0-9]):([0-9]?[0-9]):([0-9]?[0-9]).*");
		}
	}

	public String parseString(String str)
	{
		return str;
	}

	public Boolean parseBoolean(String str)
	{
		boolean retval;
		if (str != null && str.equals("1"))
			retval = true;
		else if (str != null && str.equals("0"))
			retval = false;
		else
			retval = Boolean.valueOf(str);
		return retval;
	}

	public Byte parseByte(String str)
	{
		try
		{
			Byte b;
			if (str == null || str.length() == 0)
				b = null;
			else
				b = Byte.valueOf(Byte.parseByte(str));
			return b;
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	public Short parseShort(String str)
	{
		try
		{
			Short s;
			if (str == null || str.length() == 0)
				s = null;
			else
				s = Short.valueOf(Short.parseShort(str));
			return s;
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	public Integer parseInt(String str)
	{
		try
		{
			Integer i;
			if (str == null || str.length() == 0)
				i = null;
			else
				i = Integer.valueOf(Integer.parseInt(str));
			return i;
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	public Long parseLong(String str)
	{
		try
		{
			Long l;
			if (str == null || str.length() == 0)
				l = null;
			else
				l = Long.valueOf(Long.parseLong(str));
			return l;
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	public Float parseFloat(String str)
	{
		try
		{
			Float f;
			if (str == null || str.length() == 0)
			{
				f = null;
			}
			else
			{
				str = str.replace(",", ".");
				f = Float.valueOf(Float.parseFloat(str));
			}
			return f;
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	public Double parseDouble(String str)
	{
		try
		{
			Double d;
			if (str == null || str.length() == 0)
			{
				d = null;
			}
			else
			{
				str = str.replace(",", ".");
				d = Double.valueOf(Double.parseDouble(str));
			}
			return d;
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	public byte[] parseBytes(String str)
	{
		try
		{
			byte[] b;
			if (str == null)
				b = null;
			else
				b = str.getBytes();
			return b;
		}
		catch (RuntimeException e)
		{
			return null;
		}
	}

	public BigDecimal parseBigDecimal(String str)
	{
		try
		{
			BigDecimal bd;
			if (str == null || str.length() == 0)
				bd = null;
			else
				bd = new BigDecimal(str);
			return bd;
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	/**
	 * transforms the date string into its equivalent ISO8601
	 * 
	 * @param date
	 * @param format
	 * @return
	 */
	private String makeISODate(String date, String format)
	{
		// first memorize the original order of the groups.
		format = format.toLowerCase();
		int dpos = format.indexOf('d');
		int mpos = format.indexOf('m');
		int ypos = format.indexOf('y');

		int day = 1, month = 1, year = 1;
		if (dpos > mpos)
			day += 1;
		else
			month += 1;
		if (dpos > ypos)
			day += 1;
		else
			year += 1;
		if (mpos > ypos)
			month += 1;
		else
			year += 1;

		// then build the regular expression
		Pattern part;
		Matcher m;

		part = Pattern.compile("d+");
		m = part.matcher(format);
		if (m.find())
			format = format.replace(m.group(), "([0-9]{" + (m.end() - m.start()) + ",2})");

		part = Pattern.compile("m+");
		m = part.matcher(format);
		if (m.find())
			format = format.replace(m.group(), "([0-9]{" + (m.end() - m.start()) + ",2})");

		part = Pattern.compile("y+");
		m = part.matcher(format);
		if (m.find())
			format = format.replace(m.group(), "([0-9]{" + (m.end() - m.start()) + ",4})");

		format = format + ".*";

		Pattern pattern = Pattern.compile(format);
		m = pattern.matcher(date);
		if (m.matches())
		{
			// and return the groups in ISO8601 format.
			String yearGroup = m.group(year);
			String monthGroup = m.group(month);
			if (monthGroup.length() < 2)
				monthGroup = "0" + monthGroup;
			String dayGroup = m.group(day);
			if (dayGroup.length() < 2)
				dayGroup = "0" + dayGroup;
			String retval = yearGroup + "-" + monthGroup + "-" + dayGroup;
			return retval;
		}
		else
		{
			return null;
		}
	}

	public Date parseDate(String str)
	{
		try
		{
			Date sqlResult = null;
			if (str != null && str.length() > 0)
			{
				if (simpleDateFormat != null)
				{
					java.util.Date parsedDate = simpleDateFormat.parse(str);
					long millis = parsedDate.getTime();
					sqlResult = new Date(millis);
					return sqlResult;
				}
				String isoDate = makeISODate(str, dateFormat);
				if (isoDate != null)
					sqlResult = Date.valueOf(isoDate);
			}
			return sqlResult;
		}
		catch (ParseException e)
		{
			return null;
		}
		catch (RuntimeException e)
		{
			return null;
		}
	}

	public Time parseTime(String str)
	{
		try
		{
			Time sqlResult = null;
			if (str != null && str.length() > 0)
			{
				str = str.trim();
				while (str.length() < timeFormat.length())
				{
					str = "0" + str;
				} 
				java.util.Date parsedDate = simpleTimeFormat.parse(str);
				long millis = parsedDate.getTime();
				sqlResult = new Time(millis);
			}
			return sqlResult;
		}
		catch (ParseException e)
		{
			return null;
		}
		catch (RuntimeException e)
		{
			return null;
		}
	}

	public Timestamp parseTimestamp(String str)
	{
		Timestamp result = null;
		try
		{
			if (str != null && str.length() > 0)
			{
				if (timestampFormat != null)
				{
					java.util.Date date = timestampFormat.parse(str);
					result = new Timestamp(date.getTime());
				}
				else
				{
					Matcher matcher = timestampPattern.matcher(str);
					if (matcher.matches())
					{
						int year = Integer.parseInt(matcher.group(1));
						int month = Integer.parseInt(matcher.group(2)) - 1;
						int date = Integer.parseInt(matcher.group(3));
						int hours = Integer.parseInt(matcher.group(4));
						int minutes = Integer.parseInt(matcher.group(5));
						int seconds = Integer.parseInt(matcher.group(6));
						calendar.set(year, month, date, hours, minutes, seconds);
						result = new Timestamp(calendar.getTimeInMillis());
					}
				}
			}
		}
		catch (RuntimeException e)
		{
		}
		catch (ParseException e)
		{
		}
		return result;
	}

	public InputStream parseAsciiStream(String str)
	{
		return (str == null) ? null : new ByteArrayInputStream(str.getBytes());
	}

	static protected Map<String, Class<?>> forSQLNameMap = new HashMap<String, Class<?>>()
	{
		private static final long serialVersionUID = -3037117163532338893L;
		{
			try
			{
				put("String", Class.forName("java.lang.String"));
				put("Boolean", Class.forName("java.lang.Boolean"));
				put("Byte", Class.forName("java.lang.Byte"));
				put("Short", Class.forName("java.lang.Short"));
				put("Int", Class.forName("java.lang.Integer"));
				put("Integer", Class.forName("java.lang.Integer"));
				put("Long", Class.forName("java.lang.Long"));
				put("Float", Class.forName("java.lang.Float"));
				put("Double", Class.forName("java.lang.Double"));
				put("BigDecimal", Class.forName("java.math.BigDecimal"));
				put("Date", Class.forName("java.sql.Date"));
				put("Time", Class.forName("java.sql.Time"));
				put("Timestamp", Class.forName("java.sql.Timestamp"));
				put("AsciiStream", Class.forName("java.io.InputStream"));
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
	};

	static protected Map<String, Method> converterMethodForClass = new HashMap<String, Method>()
	{
		private static final long serialVersionUID = -3037117163532338893L;
		Class<?>[] argTypes = new Class[1];
		Class<?> containerClass = null;
		{
			try
			{
				argTypes[0] = Class.forName("java.lang.String");
				containerClass = Class
						.forName("org.relique.jdbc.csv.StringConverter");
				put("String", containerClass.getMethod("parseString", argTypes));
				put("Boolean", containerClass.getMethod("parseBoolean", argTypes));
				put("Byte", containerClass.getMethod("parseByte", argTypes));
				put("Short", containerClass.getMethod("parseShort", argTypes));
				put("Int", containerClass.getMethod("parseInt", argTypes));
				put("Integer", containerClass.getMethod("parseInt", argTypes));
				put("Long", containerClass.getMethod("parseLong", argTypes));
				put("Float", containerClass.getMethod("parseFloat", argTypes));
				put("Double", containerClass.getMethod("parseDouble", argTypes));
				put("BigDecimal", containerClass.getMethod("parseBigDecimal", argTypes));
				put("Date", containerClass.getMethod("parseDate", argTypes));
				put("Time", containerClass.getMethod("parseTime", argTypes));
				put("Timestamp", containerClass.getMethod("parseTimestamp", argTypes));
				put("AsciiStream", containerClass.getMethod("parseAsciiStream", argTypes));
				/*
				 * sooner or later, maybe... put("UnicodeStream",
				 * containerClass.getMethod("parseUnicodeStream", argTypes));
				 * put("BinaryStream",
				 * containerClass.getMethod("parseBinaryStream", argTypes));
				 * put("Blob", containerClass.getMethod("parseBlob", argTypes));
				 * put("Clob", containerClass.getMethod("parseClob", argTypes));
				 * put("Array", containerClass.getMethod("parseArray",
				 * argTypes)); put("URL", containerClass.getMethod("parseURL",
				 * argTypes)); put("NCharacterStream",
				 * containerClass.getMethod("parseNCharacterStream", argTypes));
				 * put("NClob", containerClass.getMethod("parseNClob",
				 * argTypes)); put("NString",
				 * containerClass.getMethod("parseNString", argTypes));
				 * put("RowId", containerClass.getMethod("parseRowId",
				 * argTypes)); put("SQLXML",
				 * containerClass.getMethod("parseSQLXML", argTypes));
				 */
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
			catch (NoSuchMethodException e)
			{
				e.printStackTrace();
			}
		}
	};

	public Object convert(String sqlTypeName, String stringRepresentation)
	{

		Object value = stringRepresentation;
		if (sqlTypeName != null)
		{
			Object[] args = new Object[1];
			args[0] = stringRepresentation;
			try
			{
				value = converterMethodForClass.get(sqlTypeName).invoke(this, args);
			}
			catch (IllegalArgumentException e)
			{
			}
			catch (IllegalAccessException e)
			{
			}
			catch (InvocationTargetException e)
			{
			}
		}
		return value;
	}

	public Class<?> forSQLName(String string)
	{
		return forSQLNameMap.get(string);
	}

	/**
	 * Get a value that has the type of an SQL data type.
	 * 
	 * @param sqlTypeName
	 *            name of SQL data type.
	 * @return a constant value with this data type.
	 */
	public static Object getLiteralForTypeName(String sqlTypeName)
	{
		Object retval = null;
		if (sqlTypeName.equals("String"))
			retval = "";
		else if (sqlTypeName.equals("Boolean"))
			retval = Boolean.FALSE;
		else if (sqlTypeName.equals("Byte"))
			retval = Byte.valueOf((byte) 1);
		else if (sqlTypeName.equals("Short"))
			retval = Short.valueOf((short) 1);
		else if (sqlTypeName.equals("Int") || sqlTypeName.equals("Integer"))
			retval = Integer.valueOf(1);
		else if (sqlTypeName.equals("Long"))
			retval = Long.valueOf(1);
		else if (sqlTypeName.equals("Float"))
			retval = Float.valueOf(1);
		else if (sqlTypeName.equals("Double"))
			retval = Double.valueOf(1);
		else if (sqlTypeName.equals("BigDecimal"))
			retval = BigDecimal.valueOf(1);
		else if (sqlTypeName.equals("Date"))
			retval = Date.valueOf("1970-01-01");
		else if (sqlTypeName.equals("Time"))
			retval = Time.valueOf("00:00:00");
		else if (sqlTypeName.equals("Timestamp"))
			retval = Timestamp.valueOf("1970-01-01 00:00:00");
		else if (sqlTypeName.equals("AsciiStream"))
			retval = new ByteArrayInputStream(new byte[]{});
		return retval;
	}

	/**
	 * Get SQL data type of an object.
	 * 
	 * @param literal
	 *            object to get SQL data type for.
	 * @return SQL data type name.
	 */
	public static String getTypeNameForLiteral(Object literal)
	{
		String retval = null;
		if (literal instanceof String)
			retval = "String";
		else if (literal instanceof Boolean)
			retval = "Boolean";
		else if (literal instanceof Byte)
			retval = "Byte";
		else if (literal instanceof Short)
			retval = "Short";
		else if (literal instanceof Integer)
			retval = "Int";
		else if (literal instanceof Long)
			retval = "Long";
		else if (literal instanceof Float)
			retval = "Float";
		else if (literal instanceof Double)
			retval = "Double";
		else if (literal instanceof BigDecimal)
			retval = "BigDecimal";
		else if (literal instanceof Date)
			retval = "Date";
		else if (literal instanceof Time)
			retval = "Time";
		else if (literal instanceof Timestamp)
			retval = "Timestamp";
		else if (literal instanceof InputStream)
			retval = "AsciiStream";
		return retval;
	}

	public static List<Object[]> getTypeInfo()
	{
		Integer intZero = Integer.valueOf(0);
		Short shortZero = Short.valueOf((short) 0);
		Short shortMax = Short.valueOf(Short.MAX_VALUE);
		Short searchable = Short
				.valueOf((short) DatabaseMetaData.typeSearchable);
		Short nullable = Short.valueOf((short) DatabaseMetaData.typeNullable);

		ArrayList<Object[]> retval = new ArrayList<Object[]>();

		retval.add(new Object[]
		{ "String", Integer.valueOf(Types.VARCHAR), shortMax, "'", "'", null,
			nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Boolean", Integer.valueOf(Types.BOOLEAN), shortMax, null, null,
			null, nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Byte", Integer.valueOf(Types.TINYINT), shortMax, null, null, null,
			nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Short", Integer.valueOf(Types.SMALLINT), shortMax, null, null, null,
			nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Integer", Integer.valueOf(Types.INTEGER), shortMax, null, null,
			null, nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Long", Integer.valueOf(Types.BIGINT), shortMax, null, null, null,
			nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Float", Integer.valueOf(Types.FLOAT), shortMax, null, null, null,
			nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Double", Integer.valueOf(Types.DOUBLE), shortMax, null, null, null,
			nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "BigDecimal", Integer.valueOf(Types.DECIMAL), shortMax, null, null,
			null, nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Date", Integer.valueOf(Types.DATE), shortMax, "'", "'", null,
			nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Time", Integer.valueOf(Types.TIME), shortMax, "'", "'", null,
			nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Timestamp", Integer.valueOf(Types.TIMESTAMP), shortMax, null, null,
			null, nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		retval.add(new Object[]
		{ "Asciistream", Integer.valueOf(Types.CLOB), shortMax, null, null,
			null, nullable, Boolean.TRUE, searchable, Boolean.FALSE,
			Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
			intZero, intZero, intZero });

		return retval;
	}

	public static String removeQuotes(String string)
	{
		return string.replace("\"", "");
	}
}
