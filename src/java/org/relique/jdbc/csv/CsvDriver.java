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

import java.net.URLDecoder;
import java.sql.*;
import java.util.Properties;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

import org.relique.io.TableReader;

/**
 * This class implements the java.sql.Driver JDBC interface for the CsvJdbc driver.
 */
public class CsvDriver implements Driver
{
	public static final String DEFAULT_EXTENSION = ".csv";
	public static final char DEFAULT_SEPARATOR = ',';
	public static final char DEFAULT_QUOTECHAR = '"';
	public static final String DEFAULT_HEADERLINE = null;
	public static final boolean DEFAULT_SUPPRESS = false;
	public static final boolean DEFAULT_TRIM_HEADERS = true;
	public static final boolean DEFAULT_TRIM_VALUES = false;
	public static final String DEFAULT_COLUMN_TYPES = "String";
	public static final boolean DEFAULT_INDEXED_FILES = false;
	public static final String DEFAULT_TIMESTAMP_FORMAT = "YYYY-MM-DD HH:mm:ss";
	public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-DD";
	public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";
	public static final String DEFAULT_COMMENT_CHAR = null;
	public static final String DEFAULT_SKIP_LEADING_LINES = null;
	public static final String DEFAULT_IGNORE_UNPARSEABLE_LINES = "False";
	public static final String DEFAULT_FILE_TAIL_PREPEND = "False";
	public static final String DEFAULT_DEFECTIVE_HEADERS = "False";
	public static final String DEFAULT_SKIP_LEADING_DATA_LINES = "0";

	public static final String FILE_EXTENSION = "fileExtension";
	public static final String SEPARATOR = "separator";
	public static final String QUOTECHAR = "quotechar";
	public static final String HEADERLINE = "headerline";
	public static final String SUPPRESS_HEADERS = "suppressHeaders";
	public static final String TRIM_HEADERS = "trimHeaders";
	public static final String TRIM_VALUES = "trimValues";
	public static final String COLUMN_TYPES = "columnTypes";
	public static final String INDEXED_FILES = "indexedFiles";
	public static final String TIMESTAMP_FORMAT = "timestampFormat";
	public static final String DATE_FORMAT = "dateFormat";
	public static final String TIME_FORMAT = "timeFormat";
	public static final String COMMENT_CHAR = "commentChar";
	public static final String SKIP_LEADING_LINES = "skipLeadingLines";
	public static final String IGNORE_UNPARSEABLE_LINES = "ignoreNonParseableLines";
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

	public static final String FIXED_WIDTHS = "fixedWidths";

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
		DriverManager.println("CsvJdbc - CsvDriver:connect() - url=" + url);
		// check for correct url
		if (!url.startsWith(URL_PREFIX))
		{
			return null;
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
				String[] property = split[i].split("=");
				try
				{
					if (property.length == 2)
					{
						String key = URLDecoder.decode(property[0], "UTF-8");
						String value = URLDecoder.decode(property[1], "UTF-8");
						info.setProperty(key, value);
					}
					else
					{
						throw new SQLException("Invalid property: " + split[i]);
					}
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

		DriverManager.println("CsvJdbc - CsvDriver:connect() - filePath="
				+ filePath);

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
					throw new SQLException(
							"Class does not implement interface "
									+ TableReader.class.getName() + ": "
									+ className);
				}
				Object tableReaderInstance = clazz.newInstance();
				connection = new CsvConnection(
						(TableReader) tableReaderInstance, info, urlProperties);
			}
			catch (ClassNotFoundException e)
			{
				throw new SQLException(e);
			}
			catch (IllegalAccessException e)
			{
				throw new SQLException(e);
			}
			catch (InstantiationException e)
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
				throw new SQLException("Failed opening ZIP file: "
						+ zipFilename, e);
			}
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
				throw new SQLException("Directory does not exist: " + filePath);
			}
			if (!checkPath.isDirectory())
			{
				throw new SQLException("Not a directory: " + filePath);
			}

			connection = new CsvConnection(filePath, info, urlProperties);
		}
		return connection;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException
	{
		DriverManager.println("CsvJdbc - CsvDriver:accept() - url=" + url);
		return url.startsWith(URL_PREFIX);
	}

	@Override
	public boolean jdbcCompliant()
	{
		return false;
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException(
				"Driver.getParentLogger() not supported");
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
			throw new RuntimeException(
					"FATAL ERROR: Could not initialise CSV driver ! Message was: "
							+ e.getMessage());
		}
	}
}