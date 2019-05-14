/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.relique.io.CryptoFilter;
import org.relique.io.TableReader;

/**
 * This class implements the java.sql.Connection JDBC interface for the CsvJdbc driver.
 */
public class CsvConnection implements Connection
{
	/** Directory where the CSV files to use are located */
	private String path;

	/** Properties provided in connection URL */
	private String urlProperties;

	/** User-provided class that returns contents of database tables */
	private TableReader tableReader;

	/** File extension to use */
	private String extension = CsvDriver.DEFAULT_EXTENSION;

	/** Field separator to use */
	private String separator = CsvDriver.DEFAULT_SEPARATOR;

	/** Field quotechar to use */
	private Character quotechar = Character.valueOf(CsvDriver.DEFAULT_QUOTECHAR);

	/** Lookup table with headerline to use for each table */
	private HashMap<String, String> headerlines = new HashMap<String, String>();

	/** Should headers be suppressed */
	private boolean suppressHeaders = CsvDriver.DEFAULT_SUPPRESS;

	/** Is header line also fixed width? */
	private boolean isHeaderFixedWidth = CsvDriver.DEFAULT_IS_HEADER_FIXED_WIDTH;

	/** Should headers be trimmed */
	private boolean trimHeaders = CsvDriver.DEFAULT_TRIM_HEADERS;

	/** Should values be trimmed */
	private boolean trimValues = CsvDriver.DEFAULT_TRIM_VALUES;

	/** should files be grouped in one table - as if there was an index */
	private boolean indexedFiles = CsvDriver.DEFAULT_INDEXED_FILES;

	/** Lookup table with column data types for each table */
	private HashMap<String, String> columnTypes = new HashMap<String, String>();

	/** Collection of all created Statements */
	private Vector<Statement> statements = new Vector<Statement>();

	/** CharSet that should be used to read the files */
	private String charset = null;

	/** Stores whether this Connection is closed or not */
	private boolean closed;

	private String fileNamePattern;
	private String[] nameParts;
	private String timestampFormat;
	private String dateFormat;
	private String timeFormat;
	private String timeZoneName;
	private Locale locale = null;
	private String commentChar;

	private int skipLeadingLines = 0;

	private boolean ignoreUnparseableLines;

	private String missingValue;

	private boolean fileTailPrepend;

	private CryptoFilter decryptingFilter;

	private boolean defectiveHeaders;

	private int skipLeadingDataLines;

	private int transposedLines;

	private int transposedFieldsToSkip;

	private boolean autoCommit;

	private String quoteStyle;

	private ArrayList<int[]> fixedWidthColumns = null;

	private HashMap<String, Method> sqlFunctions = new HashMap<String, Method>();

	private int savepointCounter = 0;

	/**
	 * Set defaults for connection.
	 */
	private void init()
	{
		headerlines.put(null, CsvDriver.DEFAULT_HEADERLINE);
		columnTypes.put(null, CsvDriver.DEFAULT_COLUMN_TYPES);
	}

	/**
	 * Create a table of all properties with keys that start with a given
	 * prefix.
	 * 
	 * @param info
	 *            properties.
	 * @param prefix
	 *            property key prefix to match.
	 * @return matching properties, with key values having prefix removed.
	 */
	private Map<String, String> getMatchingProperties(Properties info,
		String prefix)
	{
		HashMap<String, String> retval = new HashMap<String, String>();
		for (Object o : info.keySet())
		{
			String key = o.toString();
			if (key.startsWith(prefix))
			{
				String value = info.getProperty(key);
				key = key.substring(prefix.length());
				retval.put(key, value);
			}
		}
		return retval;
	}

	private void setFunctions(Properties info) throws SQLException
	{
		String prefix = CsvDriver.FUNCTION + ".";
		for (Map.Entry<String, String> entry : getMatchingProperties(info, prefix).entrySet())
		{
			String functionName = entry.getKey().toUpperCase();
			String javaName = entry.getValue();
			try
			{
				/*
				 * SQL function name must be alphanumeric otherwise it cannot
				 * be parsed by javacc correctly.
				 */
				for (int i = 0; i < functionName.length(); i++)
				{
					char c = functionName.charAt(i);
					if (!(Character.isLetterOrDigit(c) || c == '_' || c == '.'))
					{
						throw new SQLException(CsvResources.getString("invalidFunction") +
							": " + functionName);
					}
				}

				int openParenIndex = javaName.indexOf('(');
				if (openParenIndex < 0)
					throw new SQLException(CsvResources.getString("noFunctionClass") + ": " + javaName);
				String definition = javaName.substring(0, openParenIndex).trim();
				String parameterNames = javaName.substring(openParenIndex + 1);
				int closeParenIndex = parameterNames.lastIndexOf(')');
				if (closeParenIndex < 0)
					throw new SQLException(CsvResources.getString("noFunctionClass") + ": " + javaName);
				parameterNames = parameterNames.substring(0, closeParenIndex).trim();
				int lastDotIndex = definition.lastIndexOf('.');
				if (lastDotIndex < 0)
					throw new SQLException(CsvResources.getString("noFunctionClass") + ": " + definition);
				String className = definition.substring(0, lastDotIndex);
				Class<?> clazz = Class.forName(className);
				String methodName = definition.substring(lastDotIndex + 1);
				String []parameters = new String[0];
				boolean isVarArgs = false;
				if (parameterNames.length() > 0)
				{
					parameters = parameterNames.split(",");
					for (int i = 0; i < parameters.length; i++)
					{
						/*
						 * Does this method have a variable length argument list?
						 * 
						 * In this case, the last parameter is an array containing the
						 * the remaining parameters.
						 */
						int dotIndex = parameters[i].indexOf("...");
						if (i == parameters.length - 1 && dotIndex >= 0)
						{
							parameters[i] = parameters[i].substring(0, dotIndex) + "[]";
							isVarArgs = true;
						}

						/*
						 * Just want parameter class name, not any whitespace
						 * nor any parameter name following the class name.
						 */
						parameters[i] = parameters[i].trim();
						String []split = parameters[i].split("\\s+");
						if (split.length > 1)
							parameters[i] = split[0];
					}
				}

				Method[] methods = clazz.getMethods();
				boolean methodFound = false;
				for (int i = 0; i < methods.length && methodFound == false; i++)
				{
					if (methods[i].getName().equals(methodName) &&
						(methods[i].getModifiers() & Modifier.STATIC) != 0)
					{
						Class<?>[] methodParameters = methods[i].getParameterTypes();
						boolean matchingParameters;
						matchingParameters = (methodParameters.length == parameters.length &&
							methods[i].isVarArgs() == isVarArgs);
						int j = 0;
						while (j < methodParameters.length && matchingParameters)
						{
							String methodParameterName = methodParameters[j].getSimpleName();
							if (!methodParameterName.equals(parameters[j]))
								matchingParameters = false;
							j++;
						}
						if (matchingParameters)
						{
							sqlFunctions.put(functionName, methods[i]);
							methodFound = true;
						}
					}
				}
				if (!methodFound)
				{
					throw new SQLException(CsvResources.getString("noFunctionMethod") +
						": " + javaName);
				}
			}
			catch (ClassNotFoundException e)
			{
				throw new SQLException(CsvResources.getString("noFunctionClass") + ": " + javaName, e);
			}
		}

	}

	private void setProperties(Properties info) throws SQLException
	{
		String prop;

		// set the file extension to be used
		if (info.getProperty(CsvDriver.FILE_EXTENSION) != null)
		{
			extension = info.getProperty(CsvDriver.FILE_EXTENSION);
		}
		// set the separator character to be used
		if (info.getProperty(CsvDriver.SEPARATOR) != null)
		{
			separator = info.getProperty(CsvDriver.SEPARATOR);

			// Tab character is a commonly used separator.
			// Accept tab expanded to two characters '\\' and '\t'. This
			// occurs if user types properties into a GUI text field,
			// instead of writing them as Java source code.
			if (separator.equals("\\t"))
				separator = "\t";

			if (separator.length() == 0)
				throw new SQLException(CsvResources.getString("invalid") + " " + CsvDriver.SEPARATOR + ": " + separator);
		}
		// set the quotechar character to be used
		prop = info.getProperty(CsvDriver.QUOTECHAR);
		if (prop != null)
		{
			if (prop.length() == 1)
				quotechar = Character.valueOf(prop.charAt(0));
			else if (prop.length() == 0)
				quotechar = null;
			else
				throw new SQLException(CsvResources.getString("invalid") + " " + CsvDriver.QUOTECHAR + ": " + prop);
		}
		// set the global headerline and headerline.tablename values.
		if (info.getProperty(CsvDriver.HEADERLINE) != null)
		{
			headerlines.put(null, info.getProperty(CsvDriver.HEADERLINE));
		}
		headerlines.putAll(getMatchingProperties(info, CsvDriver.HEADERLINE + "."));

		// set the header suppression flag
		if (info.getProperty(CsvDriver.SUPPRESS_HEADERS) != null)
		{
			suppressHeaders = Boolean.valueOf(info.getProperty(CsvDriver.SUPPRESS_HEADERS)).booleanValue();
		}
		// set the fixed width header flag
		if (info.getProperty(CsvDriver.IS_HEADER_FIXED_WIDTH) != null)
		{
			isHeaderFixedWidth = Boolean.valueOf(info.getProperty(CsvDriver.IS_HEADER_FIXED_WIDTH));
		}
		// set the trimValues flag
		if (info.getProperty(CsvDriver.TRIM_VALUES) != null)
		{
			trimValues = Boolean.valueOf(info.getProperty(CsvDriver.TRIM_VALUES)).booleanValue();
		}
		// default charset
		if (info.getProperty(CsvDriver.CHARSET) != null)
		{
			charset = info.getProperty(CsvDriver.CHARSET);
		}
		// set global columnTypes and columnTypes.tablename values.
		if (info.getProperty(CsvDriver.COLUMN_TYPES) != null)
		{
			columnTypes.put(null, info.getProperty(CsvDriver.COLUMN_TYPES));
		}
		columnTypes.putAll(getMatchingProperties(info, CsvDriver.COLUMN_TYPES + "."));

		// are files indexed? ()
		if (info.getProperty(CsvDriver.INDEXED_FILES) != null)
		{
			indexedFiles = Boolean.valueOf(info.getProperty(CsvDriver.INDEXED_FILES)).booleanValue();
			fileNamePattern = info.getProperty("fileTailPattern");
			String fileTailParts = info.getProperty("fileTailParts", "");
			if (!fileTailParts.isEmpty())
				nameParts = fileTailParts.split(",");
			setFileTailPrepend(Boolean.parseBoolean(info.getProperty(
				CsvDriver.FILE_TAIL_PREPEND,
				CsvDriver.DEFAULT_FILE_TAIL_PREPEND)));
		}
		// is the stream to be decrypted? ()
		// per default: no, it's unencrypted and will not be decrypted
		decryptingFilter = null;
		if (info.getProperty(CsvDriver.CRYPTO_FILTER_CLASS_NAME) != null)
		{
			String className = info.getProperty(CsvDriver.CRYPTO_FILTER_CLASS_NAME);
			try
			{
				Class<?> encrypterClass = Class.forName(className);
				String[] parameterTypes = info.getProperty("cryptoFilterParameterTypes",
					"String").split(",");
				String[] parameterStrings = info.getProperty("cryptoFilterParameters",
					"").split(",");
				StringConverter converter = new StringConverter("", "", "", "", null);
				Class<?>[] parameterClasses = new Class[parameterStrings.length];
				Object[] parameterValues = new Object[parameterStrings.length];
				for (int i = 0; i < parameterStrings.length; i++)
				{
					parameterClasses[i] = converter.forSQLName(parameterTypes[i]);
					parameterValues[i] = converter.convert(parameterTypes[i],
						parameterStrings[i]);
				}
				Constructor<?> constructor = encrypterClass.getConstructor(parameterClasses);
				decryptingFilter = (CryptoFilter) constructor.newInstance(parameterValues);
				// ignore all possible exceptions: just leave the stream
				// undecrypted.
			}
			catch (IllegalArgumentException e)
			{
				e.printStackTrace();
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (InvocationTargetException e)
			{
				e.printStackTrace();
			}
			catch (NoSuchMethodException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				throw new SQLException(CsvResources.getString("noCodecClass") + ": " + className);
			}
			if (decryptingFilter == null)
			{
				throw new SQLException(CsvResources.getString("noCryptoFilter"));
			}
		}

		setFunctions(info);

		/*
		 * for fixed width file handling fixedWidths uses format:
		 * beginIndex-endIndex,beginIndex-endIndex,... where beginIndex is the
		 * column index of the start of each column and endIndex is the optional
		 * column end position.
		 */
		String fixedWidths = info.getProperty(CsvDriver.FIXED_WIDTHS);
		if (fixedWidths != null)
		{
			fixedWidthColumns = new ArrayList<int[]>();
			String[] columnRanges = fixedWidths.split(",");
			for (int i = 0; i < columnRanges.length; i++)
			{
				int beginColumn, endColumn;
				int dashIndex = columnRanges[i].indexOf('-');
				if (dashIndex < 0)
				{
					beginColumn = endColumn = Integer.parseInt(columnRanges[i].trim());
				}
				else
				{
					beginColumn = Integer.parseInt(columnRanges[i].substring(0,
						dashIndex).trim());
					endColumn = Integer.parseInt(columnRanges[i].substring(dashIndex + 1).trim());
				}

				/*
				 * Store string indexes zero-based as we will be extracting them
				 * with String.substring().
				 */
				fixedWidthColumns.add(new int[]{beginColumn - 1, endColumn - 1});
			}
		}

		setTransposedLines(Integer.parseInt(info.getProperty(
			CsvDriver.TRANSPOSED_LINES, "0")));
		setTransposedFieldsToSkip(Integer.parseInt(info.getProperty(
			CsvDriver.TRANSPOSED_FIELDS_TO_SKIP, "0")));

		setTimestampFormat(info.getProperty(CsvDriver.TIMESTAMP_FORMAT,
			CsvDriver.DEFAULT_TIMESTAMP_FORMAT));
		setDateFormat(info.getProperty(CsvDriver.DATE_FORMAT,
			CsvDriver.DEFAULT_DATE_FORMAT));
		setTimeFormat(info.getProperty(CsvDriver.TIME_FORMAT,
			CsvDriver.DEFAULT_TIME_FORMAT));
		setTimeZoneName(info.getProperty(CsvDriver.TIME_ZONE_NAME,
			CsvDriver.DEFAULT_TIME_ZONE_NAME));
		if (info.getProperty(CsvDriver.LOCALE) != null)
		{
			prop = info.getProperty(CsvDriver.LOCALE);
			Locale []availableLocales = Locale.getAvailableLocales();
			for (int i = 0; i < availableLocales.length && locale == null; i++)
			{
				String localeString = availableLocales[i].toString();
				if (localeString.equals(prop))
					locale = availableLocales[i];
			}
			if (locale == null)
				throw new SQLException(CsvResources.getString("noLocale") + ": " + prop);
		}
		setCommentChar(info.getProperty(CsvDriver.COMMENT_CHAR,
			CsvDriver.DEFAULT_COMMENT_CHAR));
		setDefectiveHeaders(info.getProperty(CsvDriver.DEFECTIVE_HEADERS,
			CsvDriver.DEFAULT_DEFECTIVE_HEADERS));
		setSkipLeadingDataLines(info.getProperty(
			CsvDriver.SKIP_LEADING_DATA_LINES,
			CsvDriver.DEFAULT_SKIP_LEADING_DATA_LINES));
		setSkipLeadingLines(info.getProperty(CsvDriver.SKIP_LEADING_LINES,
			CsvDriver.DEFAULT_SKIP_LEADING_LINES));
		setQuoteStyle(info.getProperty(CsvDriver.QUOTE_STYLE,
			CsvDriver.DEFAULT_QUOTE_STYLE));
		setIgnoreUnparseableLines(Boolean.parseBoolean(info.getProperty(
			CsvDriver.IGNORE_UNPARSEABLE_LINES,
			CsvDriver.DEFAULT_IGNORE_UNPARSEABLE_LINES)));
		setMissingValue(info.getProperty(CsvDriver.MISSING_VALUE,
				CsvDriver.DEFAULT_MISSING_VALUE));
	}

	/**
	 * Creates a new CsvConnection that takes the supplied path and properties
	 * 
	 * @param path
	 *            directory where the CSV files are located
	 * @param info
	 *            set of properties containing custom options
	 * @param urlProperties
	 *            part of connection URL containing connection properties.
	 * @throws SQLException if connection cannot be created.
	 */
	protected CsvConnection(String path, Properties info, String urlProperties)
		throws SQLException
	{
		init();

		// validate argument(s)
		if (path == null || path.length() == 0)
		{
			throw new IllegalArgumentException(CsvResources.getString("noPath"));
		}
		this.path = path;
		this.urlProperties = urlProperties;

		// check for properties
		if (info != null)
		{
			setProperties(info);
		}
	}

	/**
	 * Creates a new database connection.
	 * 
	 * @param tableReader
	 *            user-provided class to return contents of each database table.
	 * @param info
	 *            set of properties containing custom options.
	 * @param urlProperties
	 *            part of connection URL containing connection properties.
	 * @throws SQLException if connection cannot be created.
	 */
	protected CsvConnection(TableReader tableReader, Properties info,
		String urlProperties) throws SQLException
	{
		init();
		this.tableReader = tableReader;
		this.urlProperties = urlProperties;

		// check for properties
		if (info != null)
		{
			setProperties(info);
		}
	}

	private void setQuoteStyle(String property)
	{
		// TODO Auto-generated method stub
		quoteStyle = property;
	}

	public String getQuoteStyle()
	{
		return quoteStyle;
	}

	private void setTimeZoneName(String property)
	{
		timeZoneName = property;
	}

	public String getTimeZoneName()
	{
		return timeZoneName;
	}

	public Locale getLocale()
	{
		return locale;
	}

    private void checkOpen() throws SQLException
    {
    	if (closed)
    		throw new SQLException(CsvResources.getString("closedConnection"));
    }

	@Override
	public Statement createStatement() throws SQLException
	{
		checkOpen();

		CsvStatement statement = new CsvStatement(this, ResultSet.TYPE_FORWARD_ONLY);
		statements.add(statement);
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		checkOpen();

		PreparedStatement preparedStatement = new CsvPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY);
		statements.add(preparedStatement);
		return preparedStatement;
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.prepareCall(String)");
	}

	@Override
	public String nativeSQL(String sql) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.nativeSQL(String)");
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException
	{
		checkOpen();

		this.autoCommit = autoCommit;
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		checkOpen();

		return this.autoCommit;
	}

	@Override
	public void commit() throws SQLException
	{
	}

	@Override
	public void rollback() throws SQLException
	{
	}

	private synchronized void closeStatements() throws SQLException
	{
		// close all created statements (synchronized so that closing runs only one time from one thread).
		while (statements.size() > 0)
		{
			// Closing each statement will callback and remove the statement from our list.
			statements.firstElement().close();
		}
		statements.clear();
	}

	@Override
	public void close() throws SQLException
	{
		closeStatements();

		// set this Connection as closed
		closed = true;
	}

	/**
	 * Remove closed statement from list of statements for this connection.
	 * 
	 * @param statement
	 *            statement to be removed.
	 */
	public void removeStatement(Statement statement)
	{
		statements.remove(statement);
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		checkOpen();

		return new CsvDatabaseMetaData(this);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		checkOpen();
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		checkOpen();

		return true;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException
	{
		checkOpen();

		// silently ignore this request
	}

	@Override
	public String getCatalog() throws SQLException
	{
		checkOpen();

		return null;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException
	{
		checkOpen();

		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.setTransactionIsolation(int)");
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		checkOpen();

		return Connection.TRANSACTION_NONE;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		checkOpen();

		return null;
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		checkOpen();
	}

	// --------------------------JDBC 2.0-----------------------------

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
		throws SQLException
	{
		checkOpen();

		CsvStatement statement = new CsvStatement(this, resultSetType);
		statements.add(statement);
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
		int resultSetConcurrency) throws SQLException
	{
		checkOpen();

		PreparedStatement preparedStatement = new CsvPreparedStatement(this, sql, resultSetType);
		statements.add(preparedStatement);
		return preparedStatement;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
		int resultSetConcurrency) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.prepareCall(String, int, int)");
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.getTypeMap()");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.setTypeMap(Map)");
	}

	// --------------------------JDBC 3.0-----------------------------
	@Override
	public void setHoldability(int holdability) throws SQLException
	{
		checkOpen();

		if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
			throw new SQLFeatureNotSupportedException(CsvResources.getString("unsupportedHoldability") + ": " + holdability);
	}

	@Override
	public int getHoldability() throws SQLException
	{
		checkOpen();

		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public Statement createStatement(int resultSetType,
		int resultSetConcurrency, int resultSetHoldability)
		throws SQLException
	{
		checkOpen();

		CsvStatement statement = new CsvStatement(this, resultSetType);
		statements.add(statement);
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
		int resultSetConcurrency, int resultSetHoldability)
		throws SQLException
	{
		checkOpen();

		PreparedStatement preparedStatement = new CsvPreparedStatement(this, sql, resultSetType);
		statements.add(preparedStatement);
		return preparedStatement;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
		int resultSetConcurrency, int resultSetHoldability)
		throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.prepareCall(String,int,int,int)");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.prepareStatement(String,int)");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
		throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.prepareStatement(String,int[])");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
		throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.prepareStatement(String,String[])");
	}

	@Override
	public void releaseSavepoint(Savepoint savePoint) throws SQLException
	{
		checkOpen();
	}

	@Override
	public void rollback(Savepoint savePoint) throws SQLException
	{
		checkOpen();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException
	{
		checkOpen();
		return new CsvSavepoint(savepointCounter++);
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException
	{
		checkOpen();
		return new CsvSavepoint(name);
	}

	// ---------------------------------------------------------------------
	// Properties
	// ---------------------------------------------------------------------

	/**
	 * Accessor method for the path property
	 * 
	 * @return current value for the path property
	 */
	protected String getPath()
	{
		return path;
	}

	protected TableReader getTableReader()
	{
		return tableReader;
	}

	protected String getURL()
	{
		String url;
		if (path != null)
		{
			url = CsvDriver.URL_PREFIX + path;
		}
		else if (tableReader instanceof ZipFileTableReader)
		{
			url = CsvDriver.URL_PREFIX + CsvDriver.ZIP_FILE_PREFIX +
				((ZipFileTableReader) tableReader).getZipFilename();
		}
		else
		{
			url = CsvDriver.URL_PREFIX + CsvDriver.READER_CLASS_PREFIX +
				tableReader.getClass().getName();
		}
		return url + urlProperties;
	}

	/**
	 * Accessor method for the extension property
	 * 
	 * @return current value for the extension property
	 */
	protected String getExtension()
	{
		return extension;
	}

	/**
	 * Accessor method for the separator property
	 * 
	 * @return current value for the separator property
	 */
	protected String getSeparator()
	{
		return separator;
	}

	/**
	 * Accessor method for the headerline property
	 * 
	 * @param tableName
	 *            name of database table.
	 * @return current value for the headerline property
	 */
	public String getHeaderline(String tableName)
	{
		String retval = headerlines.get(tableName);
		if (retval == null)
		{
			// Use default if no headerline defined for this table.
			retval = headerlines.get(null);
		}
		return retval;
	}

	/**
	 * Accessor method for the quotechar property
	 * 
	 * @return current value for the quotechar property
	 */
	public Character getQuotechar()
	{
		return quotechar;
	}

	/**
	 * Accessor method for the suppressHeaders property
	 * 
	 * @return current value for the suppressHeaders property
	 */
	protected boolean isSuppressHeaders()
	{
		return suppressHeaders;
	}

	protected boolean isHeaderFixedWidth()
	{
		return isHeaderFixedWidth;
	}

	public ArrayList<int[]> getFixedWidthColumns()
	{
		return fixedWidthColumns;
	}

	/**
	 * Accessor method for defectiveHeaders property.
	 * 
	 * @return true if defective headers.
	 */
	protected boolean isDefectiveHeaders()
	{
		return defectiveHeaders;
	}

	/**
	 * Accessor method for skipLeadingDataLines property.
	 * 
	 * @return number of leading data lines to skip.
	 */
	protected int getSkipLeadingDataLines()
	{
		return skipLeadingDataLines;
	}

	/**
	 * Accessor method for the charset property
	 * 
	 * @return current value for the suppressHeaders property
	 */
	protected String getCharset()
	{
		return charset;
	}

	/**
	 * Accessor method for the trimHeaders property
	 * 
	 * @return current value for the trimHeaders property
	 */
	public boolean getTrimHeaders()
	{
		return trimHeaders;
	}

	public boolean getTrimValues()
	{
		return trimValues;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob createClob() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClientInfo(String name) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException
	{
		return !closed;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	private void setDefectiveHeaders(String property)
	{
		this.defectiveHeaders = Boolean.parseBoolean(property);
	}

	private void setSkipLeadingDataLines(String property)
	{
		this.skipLeadingDataLines = Integer.parseInt(property);
	}

	/**
	 * Set column types for SQL queries.
	 * 
	 * @param columnTypes
	 *            comma-separated list of data types.
	 * @deprecated Pass columnTypes when creating driver. To be removed in a
	 *             future version.
	 */
	@Deprecated
	public void setColumnTypes(String columnTypes)
	{
		this.columnTypes.put(null, columnTypes);
	}

	public String getColumnTypes(String tableName)
	{
		String retval = columnTypes.get(tableName);
		if (retval == null)
		{
			// Use default if no columnTypes defined for this table.
			retval = columnTypes.get(null);
		}
		return retval;
	}

	/**
	 * Set flag for reading indexed files.
	 * 
	 * @param indexedFiles
	 *            flag true if indexed files are to be read.
	 * @deprecated Pass indexedFiles when creating driver. To be removed in a
	 *             future version.
	 */
	@Deprecated
	public void setIndexedFiles(boolean indexedFiles)
	{
		this.indexedFiles = indexedFiles;
	}

	public boolean isIndexedFiles()
	{
		return indexedFiles;
	}

	public String getFileNamePattern()
	{
		return fileNamePattern;
	}

	public String[] getNameParts()
	{
		return nameParts;
	}

	public void setTimestampFormat(String timestampFormat)
	{
		this.timestampFormat = timestampFormat;
	}

	public String getTimestampFormat()
	{
		return timestampFormat;
	}

	public void setDateFormat(String dateFormat)
	{
		this.dateFormat = dateFormat;
	}

	public String getDateFormat()
	{
		return dateFormat;
	}

	public void setTimeFormat(String timeFormat)
	{
		this.timeFormat = timeFormat;
	}

	public String getTimeFormat()
	{
		return timeFormat;
	}

	public void setCommentChar(String value)
	{
		if (value == null)
		{
			commentChar = null;
		}
		else if (value.equals(""))
		{
			commentChar = null;
		}
		else
		{
			commentChar = value;
		}
	}

	public String getCommentChar()
	{
		return commentChar;
	}

	private void setSkipLeadingLines(String property)
	{
		try
		{
			skipLeadingLines = Integer.parseInt(property);
		}
		catch (NumberFormatException e)
		{
			skipLeadingLines = 0;
		}
	}

	/**
	 * @return the skipLeadingLines
	 */
	public int getSkipLeadingLines()
	{
		return skipLeadingLines;
	}

	/**
	 * @param skipLeadingLines
	 *            the skipLeadingLines to set
	 */
	public void setSkipLeadingLines(int skipLeadingLines)
	{
		this.skipLeadingLines = skipLeadingLines;
	}

	public boolean isIgnoreUnparseableLines()
	{
		return ignoreUnparseableLines;
	}

	/**
	 * @param ignoreUnparseableLines
	 *            the ignoreUnparseableLines to set
	 */
	public void setIgnoreUnparseableLines(boolean ignoreUnparseableLines)
	{
		this.ignoreUnparseableLines = ignoreUnparseableLines;
	}

	public String getMissingValue()
	{
		return missingValue;
	}

	public void setMissingValue(String missingValue)
	{
		this.missingValue = missingValue;
	}

	public void setFileTailPrepend(boolean fileTailPrepend)
	{
		this.fileTailPrepend = fileTailPrepend;
	}

	public boolean isFileTailPrepend()
	{
		return fileTailPrepend;
	}

	public CryptoFilter getDecryptingCodec()
	{
		return this.decryptingFilter;
	}

	public HashMap<String, Method> getSqlFunctions()
	{
		return this.sqlFunctions;
	}

	@Override
	public NClob createNClob() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void setClientInfo(String arg0, String arg1)
		throws SQLClientInfoException
	{
		// TODO Auto-generated method stub
	}

	public int getNetworkTimeout() throws SQLException
	{
		checkOpen();

		return 0;
	}

	public void setNetworkTimeout(Executor executor, int milliseconds)
		throws SQLException
	{
		checkOpen();

		throw new SQLFeatureNotSupportedException(CsvResources.getString("methodNotSupported") + ": Connection.setNetworkTimeout(Executor,int)");
	}

	public void abort(Executor executor) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": Connection.abort(Executor)");
	}

	public String getSchema() throws SQLException
	{
		checkOpen();

		return null;
	}

	public void setSchema(String schema) throws SQLException
	{
		checkOpen();
	}

	public int getTransposedLines()
	{
		return transposedLines;
	}

	private void setTransposedLines(int i)
	{
		transposedLines = i;
	}

	public int getTransposedFieldsToSkip()
	{
		return transposedFieldsToSkip;
	}

	public void setTransposedFieldsToSkip(int i)
	{
		transposedFieldsToSkip = i;
	}

	/**
	 * Get list of table names (all files in the directory with the correct
	 * suffix).
	 * 
	 * @return list of table names.
	 * @throws SQLException if getting list of table names fails.
	 */
	public List<String> getTableNames() throws SQLException
	{
		List<String> tableNames = new ArrayList<String>();
		if (path != null)
		{
			File[] matchingFiles = new File(path).listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name)
				{
					return name.endsWith(extension);
				}
			});

			Pattern fileNameRE = null;
			if (indexedFiles)
			{
				/*
				 * Accept any filenames that match the fileTailPattern that was
				 * configured for this connection.
				 */
				fileNameRE = Pattern.compile("(.+)" + fileNamePattern);
			}

			HashSet<String> indexedTableNames = new HashSet<String>();

			for (int i = 0; i < matchingFiles.length; i++)
			{
				if (matchingFiles[i].isFile() && matchingFiles[i].canRead())
				{
					String filename = matchingFiles[i].getName();
					String tableName = filename.substring(0,
						filename.length() - extension.length());

					if (indexedFiles)
					{
						Matcher m = fileNameRE.matcher(tableName);
						if (m.matches())
						{
							/*
							 * We want only the first part of the filename, before
							 * the fileTailPattern.
							 *
							 * We use a java.util.Set so that each indexed table name
							 * is added only one time, despite comprising of several files.
							 */
							indexedTableNames.add(m.group(1));
						}
					}
					else
					{
						tableNames.add(tableName);
					}
				}
			}
			tableNames.addAll(indexedTableNames);
		}
		else
		{
			/*
			 * Get list of table names from user-provided class.
			 */
			List<String> list = tableReader.getTableNames(this);
			if (list != null)
				tableNames = list;
		}

		/*
		 * Ensure tables are always reported in the same order.
		 */
		Collections.sort(tableNames);
		return tableNames;
	}
}
