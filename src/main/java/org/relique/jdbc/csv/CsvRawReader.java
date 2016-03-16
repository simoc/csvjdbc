/*
 *	CsvJdbc - a JDBC driver for CSV files
 *	Copyright (C) 2001	Jonathan Ackerman
 *
 *	This library is free software; you can redistribute it and/or
 *	modify it under the terms of the GNU Lesser General Public
 *	License as published by the Free Software Foundation; either
 *	version 2.1 of the License, or (at your option) any later version.
 *	This library is distributed in the hope that it will be useful,
 *	but WITHOUT ANY WARRANTY; without even the implied warranty of
 *	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *	Lesser General Public License for more details.
 *	You should have received a copy of the GNU Lesser General Public
 *	License along with this library; if not, write to the Free Software
 *	Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.Vector;

import org.relique.io.DataReader;

/**
 * This class is a helper class that handles the reading and parsing of data
 * from a .csv file.
 * 
 * @author Jonathan Ackerman
 * @author Sander Brienen
 * @author Stuart Mottram (fritto)
 * @author Jason Bedell
 * @author Tomasz Skutnik
 * @author Christoph Langer
 * @author Chetan Gupta
 */

public class CsvRawReader
{
	private static final String EMPTY_STRING = "";
	private static final String ZERO_STRING = "0";

	private LineNumberReader input;
	private String tableName;
	private String tableAlias;
	private String[] columnNames;
	private String[] fieldValues;
	private String firstLineBuffer = null;
	private String separator = ",";
	private String headerLine = "";
	private boolean suppressHeaders = false;
	private boolean isHeaderFixedWidth = true;
	private Character quoteChar = Character.valueOf('"');
	private boolean trimValues = true;
	private String comment = null;
	private boolean ignoreUnparseableLines;
	private String missingValue;
	private String quoteStyle;
	private ArrayList<int []> fixedWidthColumns;
	private LinkedList<String> readAheadLines;
	private boolean readingAhead;
	private String[] previousFieldValues = null;

	public CsvRawReader(LineNumberReader in,
		String tableName,
		String tableAlias,
		String separator,
		boolean suppressHeaders,
		boolean isHeaderFixedWidth,
		Character quoteChar,
		String comment,
		String headerLine,
		boolean trimHeaders,
		boolean trimValues,
		int skipLeadingLines,
		boolean ignoreUnparseableLines,
		String missingValue,
		boolean defectiveHeaders,
		int skipLeadingDataLines,
		String quoteStyle,
		ArrayList<int []> fixedWidthColumns) throws IOException, SQLException
	{
		this.tableName = tableName;
		this.tableAlias = tableAlias;
		this.separator = separator;
		this.suppressHeaders = suppressHeaders;
		this.isHeaderFixedWidth = isHeaderFixedWidth;
		this.quoteChar = quoteChar;
		this.comment = comment;
		this.headerLine = headerLine;
		this.trimValues = trimValues;
		this.input = in;
		this.ignoreUnparseableLines = ignoreUnparseableLines;
		this.missingValue = missingValue;
		this.quoteStyle = quoteStyle;
		this.fixedWidthColumns = fixedWidthColumns;
		this.readAheadLines = new LinkedList<String>();
		this.readingAhead = false;

		for (int i = 0; i < skipLeadingLines; i++)
		{
			in.readLine();
		}

		if (this.suppressHeaders)
		{
			// column names specified by property are available. Read and use.
			if (this.headerLine != null)
			{
				this.columnNames = parseHeaderLine(this.headerLine, trimHeaders);
			}
			else
			{
				// No column names available. Read first data line and determine
				// number of columns.
				firstLineBuffer = getNextDataLine();
				String[] data = parseHeaderLine(firstLineBuffer, trimValues);
				this.columnNames = new String[data.length];
				for (int i = 0; i < data.length; i++)
				{
					this.columnNames[i] = "COLUMN" + String.valueOf(i + 1);
				}
				data = null;
				// throw away.
			}
		}
		else
		{
			String tmpHeaderLine = getNextDataLine();
			this.columnNames = parseHeaderLine(tmpHeaderLine, trimHeaders);
			// some column names may be missing and should be corrected
			if (defectiveHeaders)
				fixDefectiveHeaders();
			Set<String> uniqueNames = new HashSet<String>();
			for (int i = 0; i < this.columnNames.length; i++)
				uniqueNames.add(this.columnNames[i]);
			if (uniqueNames.size() != this.columnNames.length)
				throw new SQLException(CsvResources.getString("duplicateColumns"));
		}

		for (int i=0; i<skipLeadingDataLines; i++)
		{
			in.readLine();
		}
	}

	private void fixDefectiveHeaders()
	{
		for (int i = 0; i < this.columnNames.length; i++)
		{
			if (this.columnNames[i].length() == 0)
				this.columnNames[i] = "COLUMN" + String.valueOf(i + 1);
		}
	}

	public boolean next() throws SQLException
	{
		/*
		 * Remember String values from previous row so we can reuse them
		 * if they occur in this row too.
		 */
		previousFieldValues = fieldValues;
		fieldValues = new String[columnNames.length];
		String dataLine = null;
		try
		{
			if (suppressHeaders && (firstLineBuffer != null))
			{
				// The buffer is not empty yet, so use this first.
				dataLine = firstLineBuffer;
				firstLineBuffer = null;
			}
			else
			{
				// read new line of data from input.
				dataLine = getNextDataLine();
			}
			if (dataLine == null)
			{
				input.close();
				return false;
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
			throw new SQLException(e.toString());
		}
		String []parsedFieldValues = parseLine(dataLine, trimValues);
		if (parsedFieldValues.length < fieldValues.length && missingValue != null)
		{
			/*
			 * Add missingValue elements to make array as long as expected.
			 */
			System.arraycopy(parsedFieldValues, 0, fieldValues, 0, parsedFieldValues.length);
			for (int i = parsedFieldValues.length; i < fieldValues.length; i++)
				fieldValues[i] = missingValue;
		}
		else
		{
			fieldValues = parsedFieldValues;
		}
		return true;
	}

	public void close()
	{
		try
		{
			readAheadLines.clear();
			input.close();
			firstLineBuffer = null;
		}
		catch (Exception e)
		{
		}
	}

	/**
	 * 
	 * @return The first next data line that contains the correct amount of
	 *		   columns. An amount of column is considered correct if it matches
	 *		   columnNames or if no columnNames is given and the amount is more
	 *		   than 1.
	 * @throws IOException if reading next line fails.
	 */
	protected String getNextDataLine() throws IOException
	{
		String tmp;
		if (readAheadLines.isEmpty() == false)
			tmp = readAheadLines.removeFirst();
		else
			tmp = input.readLine();

		if (comment != null && tmp != null)
		{
			while (tmp != null && (tmp.length() == 0 || tmp.startsWith(comment)))
			{
				if (readAheadLines.isEmpty() == false)
					tmp = readAheadLines.removeFirst();
				else
					tmp = input.readLine();
			}
			// set it to 0: we don't skip data lines, only pre-header lines...
			comment = null;
		}
		if(ignoreUnparseableLines && tmp != null && missingValue == null)
		{
			try
			{
				do
				{
					/*
					 * Method parseLine() reads more lines to get the complete record
					 * for multi-line records.
					 * 
					 * Remember all the lines we read ahead so we can use them again as
					 * the next lines before reading from the file again.
					 */
					readingAhead = true;
					int fieldsCount = parseLine(tmp, true).length;
					if (columnNames != null && columnNames.length == fieldsCount)
						break; // we are satisfied
					if (columnNames == null && fieldsCount != 1)
						break; // also good enough - hopefully
					CsvDriver.writeLog("Ignoring row " + input.getLineNumber() + " Line=" + tmp);

					if (readAheadLines.isEmpty() == false)
						tmp = readAheadLines.removeFirst();
					else
						tmp = input.readLine();
				}
				while (tmp != null);
			}
			catch (SQLException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally
			{
				readingAhead = false;
			}
		}
		return tmp;
	}

	public int getLineNumber()
	{
		return input.getLineNumber();
	}

	/**
	 * Gets the columnNames attribute of the CsvReader object.
	 * 
	 * @return The columnNames value.
	 */
	public String[] getColumnNames()
	{
		return columnNames;
	}

	public int[] getColumnSizes()
	{
		int []retval;
		if (fixedWidthColumns != null)
		{
			retval = new int[fixedWidthColumns.size()];
			for (int i = 0; i < retval.length; i++)
			{
				int []columnIndexes = fixedWidthColumns.get(i);
				retval[i] = columnIndexes[1] - columnIndexes[0] + 1;
			}
		}
		else
		{
			retval = new int[columnNames.length];
			Arrays.fill(retval, DataReader.DEFAULT_COLUMN_SIZE);
		}
		return retval;
	}

	public String getTableName()
	{
		return tableName;
	}

	public String getTableAlias()
	{
		return tableAlias;
	}

	public String []getFieldValues()
	{
		return fieldValues;
	}

	/**
	 * Get the value of the column at the specified index, 0 based.
	 * 
	 * @param columnIndex
	 *			  Description of Parameter.
	 * @return The column value.
	 */
	public String getField(int columnIndex)
	{
		if (columnIndex >= fieldValues.length)
		{
			return null;
		}
		String result = fieldValues[columnIndex];
		if (result != null)
			result = result.trim();
		return result;
	}

	protected String [] parseLine(String line, boolean trimValues)
		throws SQLException
	{
		String []values;
		if (fixedWidthColumns != null)
			values = parseFixedLine(line, trimValues);
		else
			values = parseCsvLine(line, trimValues);
		return values;
	}

	private String [] parseHeaderLine(String line, boolean trimValues)
		throws SQLException
	{
		String []values;
		if (fixedWidthColumns != null && isHeaderFixedWidth)
			values = parseFixedLine(line, trimValues);
		else
			values = parseCsvLine(line, trimValues);
		return values;
	}


	private String [] parseFixedLine(String line, boolean trimValues)
		throws SQLException
	{
		String []values = new String[fixedWidthColumns.size()];
		if (line == null)
			line = "";
		for (int i = 0; i < values.length; i++)
		{
			int []columnIndexes = fixedWidthColumns.get(i);
			if (columnIndexes[0] >= line.length())
				values[i] = "";
			else if (columnIndexes[1] >= line.length())
				values[i] = line.substring(columnIndexes[0], line.length());
			else
				values[i] = line.substring(columnIndexes[0], columnIndexes[1] + 1);

			values[i] = values[i].trim();
		}
		return values;
	}

	private String rtrim(String s)
	{
		int origLen = s.length();
		int len = origLen;
		while (len > 0 && Character.isWhitespace(s.charAt(len - 1)))
		{
			len--;
		}
		if (len == origLen)
			return s;
		else
			return s.substring(0, len);
	}

	private boolean isQuoteChar(char c)
	{
		return quoteChar != null && c == quoteChar.charValue();
	}

	private String createStringValue(StringBuilder columnValue, int columnIndex)
	{
		String s;

		/*
		 * Optimise for the two most frequent values in CSV files to avoid
		 * creating unnecessary String objects.
		 */
		int len = columnValue.length();
		if (len == 0)
		{
			s = EMPTY_STRING;
		}
		else if (len == 1 && columnValue.charAt(0) == '0')
		{
			s = ZERO_STRING;
		}
		else
		{
			s = columnValue.toString();
			if (previousFieldValues != null && previousFieldValues.length > columnIndex)
			{
				if (previousFieldValues[columnIndex] != null && previousFieldValues[columnIndex].equals(s))
				{
					/*
					 * Reuse String from previous row with same value to reduce number of
					 * allocated java.lang.String objects.
					 */
					s = previousFieldValues[columnIndex];
				}
			}
		}
		return s;
	}

	/**
	 * splits <b>line</b> into the String[] it contains.
	 * Stuart Mottram added the code for handling line breaks in fields.
	 * 
	 * @param line the line to parse
	 * @param trimValues tells whether to remove leading and trailing spaces
	 * @return line split into fields.
	 * @throws SQLException
	 */
	private String[] parseCsvLine(String line, boolean trimValues)
			throws SQLException
	{
		// TODO: quoteChar should be recognized ONLY when close to separator. 
		Vector<String> values = new Vector<String>();
		boolean inQuotedString = false;
		int quotedLineNumber = 0;
		StringBuilder value = new StringBuilder(32);
		String orgLine = line;
		int currentPos = 0;
		int fullLine = 0;

		while (fullLine == 0)
		{
			currentPos = 0;
			line += separator; // this way all fields are separator-terminated
			while (currentPos < line.length())
			{
				char currentChar = line.charAt(currentPos);
				if (value.length() == 0 && isQuoteChar(currentChar)
						&& !inQuotedString)
				{
					// acknowledge quoteChar only at beginning of value.
					inQuotedString = true;
					quotedLineNumber = input.getLineNumber();
				}
				else if (currentChar == '\\' && "C".equals(quoteStyle))
				{
					// in C quoteStyle \\ escapes any character.
					char nextChar = line.charAt(currentPos + 1);
					value.append(nextChar);
					currentPos++;
				}
				else if (isQuoteChar(currentChar))
				{
					char nextChar = line.charAt(currentPos + 1);
					if (!inQuotedString)
					{
						// accepting the single quoteChar because the whole
						// value is not quoted.
						value.append(quoteChar.charValue());
					}
					else if (isQuoteChar(nextChar))
					{
						value.append(quoteChar.charValue());
						if ("SQL".equals(quoteStyle))
						{
							// doubled quoteChar in quoted strings collapse to
							// one single quoteChar in SQL quotestyle
							currentPos++;
						}
					}
					else
					{
						while (trimValues &&
							atSeparator(line, currentPos + 1) == false &&
							Character.isWhitespace(nextChar) &&
							currentPos + 2 < line.length())
						{
							// Skip trailing whitespace after quoted value before next separator
							nextChar = line.charAt(currentPos + 2);
							currentPos++;
						}
						if (atSeparator(line, currentPos + 1) == false)
						{
							throw new SQLException(CsvResources.getString("expectedSeparator") + ": " +
								input.getLineNumber() + " " + (currentPos + 1) +
								": " + orgLine);
						}

						values.add(createStringValue(value, values.size()));
						value.setLength(0);
						inQuotedString = false;
						currentPos += separator.length();
					}
				}
				else
				{
					if (atSeparator(line, currentPos))
					{
						if (inQuotedString)
						{
							value.append(currentChar);
						}
						else
						{
							if (trimValues)
							{
								values.add(rtrim(createStringValue(value, values.size())));
							}
							else
							{
								values.add(createStringValue(value, values.size()));
							}
							value.setLength(0);

							if (separator.length() > 1)
							{
								/*
								 * Skip other characters in separator too.
								 */
								currentPos += separator.length() - 1;
							}
						}
					}
					else if (trimValues &&
						Character.isWhitespace(currentChar) &&
						value.length() == 0 &&
						inQuotedString == false)
					{
						// Skip leading whitespace in field
					}
					else
					{
						// default action
						value.append(currentChar);
					}
				}
				currentPos++;
			}
			if (inQuotedString)
			{
				// Line ended while looking for matching quoteChar. This means
				// we are inside of a field (not yet fullLine).
				// Remove extra separator added at start.
				value = new StringBuilder(value.substring(0, value.length() - 1));
				try
				{
					String additionalLine;
					if (readingAhead)
					{
						additionalLine = input.readLine();

						/*
						 * Remember each line we read ahead -- we may have to re-read
						 * these lines later.
						 */
						if (additionalLine != null)
							readAheadLines.addLast(additionalLine);
					}
					else
					{
						if (readAheadLines.isEmpty() == false)
							additionalLine = readAheadLines.removeFirst();
						else
							additionalLine = input.readLine();
					}

					if (additionalLine == null)
					{
						throw new SQLException(CsvResources.getString("eofInQuotes") + ": " +
							quotedLineNumber);
					}
					line = "\n" + additionalLine;
					if (orgLine == firstLineBuffer)
					{
						// We are reading and remembering the first record to
						// determine the number of columns in the file.
						// Append any extra lines we read for first record to
						// the buffer too.
						firstLineBuffer += "\n" + additionalLine;
					}
				}
				catch (IOException e)
				{
					throw new SQLException(e.toString());
				}
			}
			else
			{
				fullLine = 1;
			}

		}
		String[] retVal = new String[values.size()];
		values.copyInto(retVal);
		return retVal;
	}

	private boolean atSeparator(String line, int currentPos)
	{
		boolean matchesSeparator;
		
		/*
		 * Quicker to compare just the current character for the
		 * normal case of a single character separator.
		 */
		if (separator.length() == 1)
			matchesSeparator = (line.charAt(currentPos) == separator.charAt(0));
		else
			matchesSeparator = line.regionMatches(currentPos, separator, 0, separator.length());
		return matchesSeparator;
	}
}
