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
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;

import org.relique.io.TableReader;

/**
 * This class implements the java.sql.Driver JDBC interface for the CsvJdbc driver.
 */
public class CsvDriver implements Driver
{
	public static final String DEFAULT_EXTENSION = ".csv";
	public static final String DEFAULT_SEPARATOR = ",";
	public static final char DEFAULT_QUOTECHAR = '"';
	public static final String DEFAULT_HEADERLINE = null;
	public static final boolean DEFAULT_SUPPRESS = false;
	public static final boolean DEFAULT_IS_HEADER_FIXED_WIDTH = true;
	public static final boolean DEFAULT_TRIM_HEADERS = true;
	public static final boolean DEFAULT_TRIM_VALUES = false;
	public static final String DEFAULT_COLUMN_TYPES = "String";
	public static final boolean DEFAULT_INDEXED_FILES = false;
	public static final String DEFAULT_TIMESTAMP_FORMAT = null;
	public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-DD";
	public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";
	public static final boolean DEFAULT_USE_DATE_TIME_FORMATTER = false;
	public static final String DEFAULT_COMMENT_CHAR = null;
	public static final String DEFAULT_SKIP_LEADING_LINES = null;
	public static final String DEFAULT_MAX_DATA_LINES = "0";
	public static final String DEFAULT_IGNORE_UNPARSEABLE_LINES = "False";
	public static final String DEFAULT_MISSING_VALUE = null;
	public static final String DEFAULT_FILE_TAIL_PREPEND = "False";
	public static final String DEFAULT_DEFECTIVE_HEADERS = "False";
	public static final String DEFAULT_SKIP_LEADING_DATA_LINES = "0";

	public static final String FILE_EXTENSION = "fileExtension";
	public static final String SEPARATOR = "separator";
	public static final String QUOTECHAR = "quotechar";
	public static final String HEADERLINE = "headerline";
	public static final String SUPPRESS_HEADERS = "suppressHeaders";
	public static final String IS_HEADER_FIXED_WIDTH = "isHeaderFixedWidth";
	public static final String TRIM_HEADERS = "trimHeaders";
	public static final String TRIM_VALUES = "trimValues";
	public static final String COLUMN_TYPES = "columnTypes";
	public static final String INDEXED_FILES = "indexedFiles";
	public static final String TIMESTAMP_FORMAT = "timestampFormat";
	public static final String DATE_FORMAT = "dateFormat";
	public static final String TIME_FORMAT = "timeFormat";
	public static final String LOCALE = "locale";
	public static final String USE_DATE_TIME_FORMATTER = "useDateTimeFormatter";
	public static final String COMMENT_CHAR = "commentChar";
	public static final String SKIP_LEADING_LINES = "skipLeadingLines";
	public static final String MAX_DATA_LINES = "maxDataLines";
	public static final String IGNORE_UNPARSEABLE_LINES = "ignoreNonParseableLines";
	public static final String MISSING_VALUE = "missingValue";
	public static final String FILE_TAIL_PREPEND = "fileTailPrepend";
	public static final String DEFECTIVE_HEADERS = "defectiveHeaders";
	public static final String SKIP_LEADING_DATA_LINES = "skipLeadingDataLines";
	public static final String TRANSPOSED_LINES = "transposedLines";
	public static final String TRANSPOSED_FIELDS_TO_SKIP = "transposedFieldsToSkip";

	public static final String CHARSET = "charset";
	public final static String URL_PREFIX = "jdbc:relique:csv:";
	public static final String CRYPTO_FILTER_CLASS_NAME = "cryptoFilterClassName";

	public static final String TIME_ZONE_NAME = "timeZoneName";
	public static final String DEFAULT_TIME_ZONE_NAME = "UTC";
	// choosing Rome makes sure we change chronology from Julian to Gregorian on
	// 1582-10-04/15, as SQL does.
	public static final String QUOTE_STYLE = "quoteStyle";
	public static final String DEFAULT_QUOTE_STYLE = "SQL";

	public static final String READER_CLASS_PREFIX = "class:";
	public static final String ZIP_FILE_PREFIX = "zip:";
	public static final String CLASSPATH_PREFIX = "classpath:";

	public static final String FIXED_WIDTHS = "fixedWidths";

	public static final String FUNCTION = "function";

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
		throws SQLException
	{
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion()
	{
		return 1;
	}

	@Override
	public int getMinorVersion()
	{
		return 0;
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException
	{
		writeLog("CsvDriver:connect() - url=" + url);
		// check for correct url
		if (!url.startsWith(URL_PREFIX))
		{
			return null;
		}

		Enumeration<Object> keys = info.keys();
		while (keys.hasMoreElements())
		{
			String key = keys.nextElement().toString();
			writeLog("CsvDriver:connect() - property " + key + "=" + info.getProperty(key));
		}

		// strip any properties from end of URL and set them as additional
		// properties
		String urlProperties = "";
		int questionIndex = url.indexOf('?');
		if (questionIndex >= 0)
		{
			info = new Properties(info);
			urlProperties = url.substring(questionIndex);
			String[] split = urlProperties.substring(1).split("&");
			for (int i = 0; i < split.length; i++)
			{
				int equalsIndex = split[i].indexOf("=");
				if (equalsIndex <= 0)
					throw new SQLException(CsvResources.getString("invalidProperty") + ": " + split[i]);
				int lastEqualsIndex = split[i].lastIndexOf("=");
				if (lastEqualsIndex != equalsIndex)
					throw new SQLException(CsvResources.getString("invalidProperty") + ": " + split[i]);

				try
				{
					String key = URLDecoder.decode(split[i].substring(0, equalsIndex), "UTF-8");
					String value = URLDecoder.decode(split[i].substring(equalsIndex + 1), "UTF-8");
					info.setProperty(key, value);
				}
				catch (UnsupportedEncodingException e)
				{
					// we know UTF-8 is available
				}
			}
			url = url.substring(0, questionIndex);
		}
		// get filepath from url
		String filePath = url.substring(URL_PREFIX.length());

		writeLog("CsvDriver:connect() - filePath=" + filePath);

		CsvConnection connection;
		if (filePath.startsWith(READER_CLASS_PREFIX))
		{
			String className = filePath.substring(READER_CLASS_PREFIX.length());
			try
			{
				Class<?> clazz = Class.forName(className);

				/*
				 * Check that class implements our interface.
				 */
				Class<?>[] interfaces = clazz.getInterfaces();
				boolean isInterfaceImplemented = false;
				for (int i = 0; i < interfaces.length
						&& (!isInterfaceImplemented); i++)
				{
					if (interfaces[i].equals(TableReader.class))
						isInterfaceImplemented = true;
				}

				if (!isInterfaceImplemented)
				{
					
					throw new SQLException(CsvResources.getString("interfaceNotImplemented") +
						": " + TableReader.class.getName() + ": " + className);
				}
				Object tableReaderInstance = clazz.getConstructor().newInstance();
				connection = new CsvConnection((TableReader)tableReaderInstance, info, urlProperties);
			}
			catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException
					| NoSuchMethodException e)
			{
				throw new SQLException(e);
			}
		}
		else if (filePath.startsWith(ZIP_FILE_PREFIX))
		{
			String zipFilename = filePath.substring(ZIP_FILE_PREFIX.length());
			try
			{
				ZipFileTableReader zipFileTableReader = new ZipFileTableReader(
						zipFilename, info.getProperty(CHARSET));
				connection = new CsvConnection(zipFileTableReader, info,
						urlProperties);
				zipFileTableReader.setExtension(connection.getExtension());
			}
			catch (IOException e)
			{
				throw new SQLException(CsvResources.getString("zipOpenError") + ": " +
					zipFilename, e);
			}
		}
		else if (filePath.startsWith(CLASSPATH_PREFIX))
		{
			String path = filePath.substring(CLASSPATH_PREFIX.length());
			ClasspathTableReader classpathTableReader = new ClasspathTableReader(
					path, info.getProperty(CHARSET));
			connection = new CsvConnection(classpathTableReader, info, urlProperties);
			classpathTableReader.setExtension(connection.getExtension());
		}
		else
		{
			if (!filePath.endsWith(File.separator))
			{
				filePath += File.separator;
			}

			// check if filepath is a correct path.
			File checkPath = new File(filePath);
			if (!checkPath.exists())
			{
				throw new SQLException(CsvResources.getString("dirNotFound") + ": " + filePath);
			}
			if (!checkPath.isDirectory())
			{
				throw new SQLException(CsvResources.getString("dirNotFound") + ": " + filePath);
			}

			connection = new CsvConnection(filePath, info, urlProperties);
		}
		return connection;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException
	{
		writeLog("CsvDriver:accept() - url=" + url);
		return url.startsWith(URL_PREFIX);
	}

	@Override
	public boolean jdbcCompliant()
	{
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException(CsvResources.getString("methodNotSupported") +
			": Driver.getParentLogger()");
	}

	public static void writeLog(String message)
	{
		PrintWriter logWriter = DriverManager.getLogWriter();
		if (logWriter != null)
			logWriter.println("CsvJdbc: " + message);
	}

	/**
	 * Convenience method to write a ResultSet to a CSV file.
	 * Output CSV file has the same format as the CSV file that is
	 * being queried, so that it can be used for later SQL queries.
	 * @param resultSet JDBC ResultSet to write.
	 * @param out open stream to write to.
	 * @param writeHeaderLine if true, the column names are written as first line.
	 * @throws SQLException if writing to CSV file fails.
	 */
	public static void writeToCsv(ResultSet resultSet, PrintStream out, boolean writeHeaderLine)
		throws SQLException
	{
		String separator = DEFAULT_SEPARATOR;
		Character quoteChar = Character.valueOf(DEFAULT_QUOTECHAR);
		String quoteStyle = DEFAULT_QUOTE_STYLE;
		String dateFormat = DEFAULT_DATE_FORMAT;
		String timeFormat = DEFAULT_TIME_FORMAT;
		String timestampFormat = DEFAULT_TIMESTAMP_FORMAT;
		String timeZoneName = DEFAULT_TIME_ZONE_NAME;
		boolean useDateTimeFormatter = DEFAULT_USE_DATE_TIME_FORMATTER;
		Locale locale = null;

		if (resultSet instanceof CsvResultSet)
		{
			/*
			 * Use same formatting options as the CSV file this ResultSet was read from.
			 */
			CsvResultSet csvResultSet = (CsvResultSet)resultSet;
			CsvConnection csvConnection = (CsvConnection)csvResultSet.getStatement().getConnection();
			separator = csvConnection.getSeparator();
			quoteChar = csvConnection.getQuotechar();
			quoteStyle = csvConnection.getQuoteStyle();
			dateFormat = csvConnection.getDateFormat();
			timeFormat = csvConnection.getTimeFormat();
			timestampFormat = csvConnection.getTimestampFormat();
			timeZoneName = csvConnection.getTimeZoneName();
			locale = csvConnection.getLocale();
			useDateTimeFormatter = csvConnection.getUseDateTimeFormatter();
		}

		StringConverter converter = new StringConverter(dateFormat, timeFormat, timestampFormat, timeZoneName, locale, useDateTimeFormatter);

		ResultSetMetaData meta = null;
		int columnCount = 0;

		/*
		 * Write each row of ResultSet.
		 */
		while (resultSet.next())
		{
			if (meta == null)
			{
				meta = resultSet.getMetaData();
				columnCount = meta.getColumnCount();
				if (writeHeaderLine)
				{			
					for (int i = 1; i <= columnCount; i++)
					{
						if (i > 1)
							out.print(separator);
						out.print(meta.getColumnName(i));
					}
					out.println();
				}
			}

			for (int i = 1; i <= columnCount; i++)
			{
				if (i > 1)
					out.print(separator);
				String value = null;

				/*
				 * Use same dateFormat, timeFormat and timestampFormat for output as the input CSV file.
				 */
				int columnType = meta.getColumnType(i);
				if (columnType == Types.DATE)
				{
					Date d = resultSet.getDate(i);
					if (d != null)
						value = converter.formatDate(d);
				}
				else if (columnType == Types.TIME)
				{
					Time t = resultSet.getTime(i);
					if (t != null)
						value = converter.formatTime(t);
				}
				else if (columnType == Types.TIMESTAMP)
				{
					Timestamp timestamp = resultSet.getTimestamp(i);
					if (timestamp != null)
						value = converter.formatTimestamp(timestamp);
				}
				else
				{
					value = resultSet.getString(i);
				}
				if (value != null)
				{
					if (quoteChar != null)
						value = addQuotes(value, separator, quoteChar.charValue(), quoteStyle);
					out.print(value);
				}
			}
			out.println();
		}
		if (meta == null && writeHeaderLine)
		{
			meta = resultSet.getMetaData();
			columnCount = meta.getColumnCount();

			for (int i = 1; i <= columnCount; i++)
			{
				if (i > 1)
					out.print(separator);
				out.print(meta.getColumnName(i));
			}
			out.println();
		}
		out.flush();
	}

	private static String addQuotes(String value, String separator, char quoteChar, String quoteStyle)
	{
		/*
		 * Escape all quote chars embedded in the string.
		 */
		if (quoteStyle.equals("C"))
		{
			value = value.replace("\\", "\\\\");
			value = value.replace("" + quoteChar, "\\" + quoteChar);
		}
		else
		{
			value = value.replace("" + quoteChar, "" + quoteChar + quoteChar);
		}

		/*
		 * Surround value with quotes if it contains any special characters.
		 */
		if (value.indexOf(separator) >= 0 || value.indexOf(quoteChar) >= 0 ||
			value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0)
		{
			value = quoteChar + value + quoteChar;
		}
		return value;
	}

	// This static block inits the driver when the class is loaded by the JVM.
	static
	{
		try
		{
			java.sql.DriverManager.registerDriver(new CsvDriver());
		}
		catch (SQLException e)
		{
			throw new RuntimeException(CsvResources.getString("initFailed") + ": " + e.getMessage());
		}
	}
}
