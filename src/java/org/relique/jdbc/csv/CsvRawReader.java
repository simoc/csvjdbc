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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.relique.io.CryptoFilter;

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
 * @created 25 November 2001
 * @version $Id: CsvRawReader.java,v 1.9 2011/10/25 17:24:38 simoc Exp $
 */

public class CsvRawReader {

	protected BufferedReader input;
	protected String tableAlias;
	protected String[] columnNames;
	protected String[] fieldValues;
	protected java.lang.String buf = null;
	protected char separator = ',';
	protected String headerLine = "";
	protected boolean suppressHeaders = false;
	protected char quoteChar = '"';
	protected String extension = CsvDriver.DEFAULT_EXTENSION;
	protected boolean trimHeaders = true;
	protected char commentChar = 0;
	private boolean ignoreUnparseableLines;
	protected CryptoFilter filter;
	private String quoteStyle;

	/**
	 * Insert the method's description here.
	 * 
	 * Creation date: (6-11-2001 15:02:42)
	 * 
	 * @param fileName
	 *            java.lang.String
	 * @param separator
	 *            char
	 * @param suppressHeaders
	 *            boolean
	 * @param quoteChar
	 *            char
	 * @param filter the decrypting filter
	 * @param defectiveHeaders 
	 * @param skipLeadingDataLines 
	 * @exception java.lang.Exception
	 *                The exception description.
	 * @throws SQLException
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 * @since
	 */
	public CsvRawReader(BufferedReader in, String tableAlias, char separator,
			boolean suppressHeaders, char quoteChar, char commentChar,
			String headerLine, String extension, boolean trimHeaders, 
			int skipLeadingLines, boolean ignoreUnparseableLines, CryptoFilter filter, 
			boolean defectiveHeaders, int skipLeadingDataLines, String quoteStyle)
			throws IOException, SQLException {
		this.tableAlias = tableAlias;
		this.separator = separator;
		this.suppressHeaders = suppressHeaders;
		this.quoteChar = quoteChar;
		this.commentChar = commentChar;
		this.headerLine = headerLine;
		this.extension = extension;
		this.trimHeaders = trimHeaders;
		this.input = in;
		this.ignoreUnparseableLines = ignoreUnparseableLines;
		this.filter = filter;
		this.quoteStyle = quoteStyle;

		for (int i=0; i<skipLeadingLines; i++){
			in.readLine();
		}

		if (this.suppressHeaders) {
			// column names specified by property are available. Read and use.
			if (this.headerLine != null) {
				this.columnNames = parseCsvLine(this.headerLine, trimHeaders);
			} else {
				// No column names available. Read first data line and determine
				// number of columns.
				buf = getNextDataLine();
				String[] data = parseCsvLine(buf, false);
				this.columnNames = new String[data.length];
				for (int i = 0; i < data.length; i++) {
					this.columnNames[i] = "COLUMN" + String.valueOf(i + 1);
				}
				data = null;
				// throw away.
			}
		} else {
			String tmpHeaderLine = getNextDataLine();
			this.columnNames = parseCsvLine(tmpHeaderLine, trimHeaders);
			Set uniqueNames = new HashSet();
			for (int i = 0; i < this.columnNames.length; i++)
				uniqueNames.add(this.columnNames[i]);
			if (uniqueNames.size() != this.columnNames.length)
				throw new SQLException("Table contains duplicate column names");
		}
		// some column names may be missing and should be corrected
		if (defectiveHeaders)
			for (int i = 0; i < this.columnNames.length; i++)
				if (this.columnNames[i].length() == 0)
					this.columnNames[i] = "COLUMN" + String.valueOf(i + 1);

		for (int i=0; i<skipLeadingDataLines; i++){
			in.readLine();
		}
	}

	/**
	 *Description of the Method
	 * 
	 * @return Description of the Returned Value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public boolean next() throws SQLException {
		fieldValues = new String[columnNames.length];
		String dataLine = null;
		try {
			if (suppressHeaders && (buf != null)) {
				// The buffer is not empty yet, so use this first.
				dataLine = buf;
				buf = null;
			} else {
				// read new line of data from input.
				dataLine = getNextDataLine();
			}
			if (dataLine == null) {
				input.close();
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new SQLException(e.toString());
		}
		fieldValues = parseCsvLine(dataLine, false);
		return true;
	}

	/**
	 *Description of the Method
	 * 
	 * @since
	 */
	public void close() {
		try {
			input.close();
			buf = null;
		} catch (Exception e) {
		}
	}

	/**
	 * 
	 * @return The first next data line that contains the correct amount of
	 *         columns. An amount of column is considered correct if it matches
	 *         columnNames or if no columnNames is given and the amount is more
	 *         than 1.
	 * @throws IOException
	 */
	protected String getNextDataLine() throws IOException {
		String tmp = input.readLine();
		if (commentChar != 0 && tmp != null) {
			while (tmp != null && (tmp.length() == 0 || tmp.charAt(0) == commentChar))
				tmp = input.readLine();
			// set it to 0: we don't skip data lines, only pre-header lines...
			commentChar = 0;
		}
		if(ignoreUnparseableLines && tmp != null) {
			try {
				do {
					int fieldsCount = this.parseCsvLine(tmp, true).length;
					if (columnNames != null && columnNames.length == fieldsCount)
						break; // we are satisfied
					if (columnNames == null && fieldsCount != 1)
						break; // also good enough - hopefully
					tmp = input.readLine();
				} while (tmp != null);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return tmp;
	}

	/**
	 *Gets the columnNames attribute of the CsvReader object
	 * 
	 * @return The columnNames value
	 * @since
	 */
	public String[] getColumnNames() {
		return columnNames;
	}

	public String getTableAlias() {
		return tableAlias;
	}

	/**
	 * Get the value of the column at the specified index, 0 based.
	 * 
	 * @param columnIndex
	 *            Description of Parameter
	 * @return The column value
	 * @since
	 */
	public String getField(int columnIndex) throws SQLException {
		if (columnIndex >= fieldValues.length) {
			return null;
		}
		String result = fieldValues[columnIndex];
		if (result != null)
			result = result.trim();
		return result;
	}

	/**
	 * splits <b>line</b> into the String[] it contains.
	 * Stuart Mottram added the code for handling line breaks in fields.
	 * 
	 * @param line the line to parse
	 * @param trimValues tells whether to remove leading and trailing spaces
	 * @return
	 * @throws SQLException
	 */
	protected String[] parseCsvLine(String line, boolean trimValues)
			throws SQLException {
		// TODO: quoteChar should be recognized ONLY when close to separator. 
		Vector values = new Vector();
		boolean inQuotedString = false;
		String value = "";
		String orgLine = line;
		int currentPos = 0;
		int fullLine = 0;

		while (fullLine == 0) {
			currentPos = 0;
			line += separator; // this way fields are separator-terminated
			while (currentPos < line.length()) {
				char currentChar = line.charAt(currentPos);
				if (value.length() == 0 && currentChar == quoteChar
						&& !inQuotedString) {
					// acknowledge quoteChar only at beginning of value.
					inQuotedString = true;
				} else if (currentChar == '\\' && "C".equals(quoteStyle)) {
					// in C quoteStyle \\ escapes any character.
					char nextChar = line.charAt(currentPos + 1);
					value += nextChar;
					currentPos++;
				} else if (currentChar == quoteChar) {
					char nextChar = line.charAt(currentPos + 1);
					if (!inQuotedString) {
						// accepting the single quoteChar because the whole
						// value is not quoted.
						value += quoteChar;
					} else if (nextChar == quoteChar) {
						value += quoteChar;
						if ("SQL".equals(quoteStyle)) {
							// doubled quoteChar in quoted strings collapse to
							// one single quoteChar in SQL quotestyle
							currentPos++;
						}
					} else {
						if (nextChar != separator) {
							throw new SQLException("Expecting " + separator
									+ " in position " + (currentPos + 1)
									+ ". Line=" + orgLine);
						}
						if (trimValues) {
							values.add(value.trim());
						} else {
							values.add(value);
						}
						value = "";
						inQuotedString = false;
						currentPos++;
					}
				} else {
					if (currentChar == separator) {
						if (inQuotedString) {
							value += currentChar;
						} else {
							if (trimValues) {
								values.add(value.trim());
							} else {
								values.add(value);
							}
							value = "";
						}
					} else {
						// default action
						value += currentChar;
					}
				}
				currentPos++;
			}
			if (inQuotedString) {
				// Line ended while looking for matching quoteChar. This means
				// we are inside of a field (not yet fullLine).
				// Remove extra separator added at start.
				value = value.substring(0, value.length() - 1);
				try {
					String additionalLine = input.readLine();
					if (additionalLine == null) 
						throw new SQLException("EOF reached inside quoted mode");
					line = "\n" + additionalLine;
				} catch (IOException e) {
					throw new SQLException(e.toString());
				}
			} else {
				fullLine = 1;
			}

		}
		String[] retVal = new String[values.size()];
		values.copyInto(retVal);
		return retVal;
	}

}
