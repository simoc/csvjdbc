/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
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
import java.io.UnsupportedEncodingException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *This class Class provides facility to navigate on the Result Set.
 * 
 * @author Chetan Gupta
 * @version $Id: CSVScrollableReader.java,v 1.9 2008/11/19 15:52:05 mfrasca Exp
 *          $
 */
public class CSVScrollableReader extends CSVReaderAdapter {
	// ---------------------------------------------------------------------
	// Traversal/Positioning
	// ---------------------------------------------------------------------
	private static final int FIRST_RECORD = 0;
	private ArrayList alRecords = null;
	private int iRecordNo = 0;

	/**
	 * Can be put in adapter apart from the last line
	 * 
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
	 * @throws SQLException
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 * @exception java.lang.Exception
	 *                The exception description.
	 * @since
	 */
	public CSVScrollableReader(BufferedReader fileName, char separator,
			boolean suppressHeaders, char quoteChar,
			String headerLine, String extension, boolean trimHeaders,
			List queryEnvironment, ExpressionParser whereClause)
			throws IOException, SQLException {

		super(fileName, separator, suppressHeaders, quoteChar, (char)0,
				headerLine, extension, trimHeaders);

		loopAndFetchData(input, buf, queryEnvironment, whereClause);
		iRecordNo = FIRST_RECORD - 1;
	}

	private void loopAndFetchData(BufferedReader input, String buf,
			List queryEnvironment, ExpressionParser whereClause)
			throws SQLException {
		alRecords = new ArrayList();
		String dataLine = null;
		List usedColumns = null;
		try {
			usedColumns = whereClause.usedColumns();
		} catch (NullPointerException e) {
			usedColumns = new ArrayList();
		}
		try {
			while (true) {
				fieldValues = new String[columnNames.length];
				dataLine = null;
				if (suppressHeaders && (buf != null)) {
					// The buffer is not empty yet, so use this first.
					dataLine = buf;
					buf = null;
				} else {
					// read new line of data from input.
					dataLine = input.readLine();
				}
				if (dataLine == null) {
					break;
				}
				fieldValues = parseCsvLine(dataLine, false);
				Map recordEnvironment = new HashMap();
				for (int i = 0; i < columnNames.length; i++) {
					String key = columnNames[i].toUpperCase();
					String value = fieldValues[i];
					recordEnvironment.put(key, value);
				}
				Map objectEnvironment = new HashMap();
				for (int i = 0; i < queryEnvironment.size(); i++) {
					Object[] o = (Object[]) queryEnvironment.get(i);
					String key = (String) o[0];
					Object value = ((Expression) o[1]).eval(recordEnvironment);
					objectEnvironment.put(key.toUpperCase(), value);
				}
				for (int i = 0; i < usedColumns.size(); i++) {
					String key = (String) usedColumns.get(i);
					key = key.toUpperCase();
					if (!objectEnvironment.containsKey(key)) {
						objectEnvironment.put(key, recordEnvironment.get(key));
					}
				}
				if (whereClause == null || whereClause.isTrue(objectEnvironment))
					alRecords.add(fieldValues);
			}
		} catch (IOException e) {
			throw new SQLException(e.toString());
		} finally {
			try {
				input.close();
			} catch (IOException e) {
			}
		}
	}

	/**
	 * Method close.
	 */
	public void close() {
		alRecords = null;
		try {
			input.close();
		} catch (IOException e) {
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
		++iRecordNo;

		return readData();
	}

	/**
	 * Moves the cursor to the previous row in this <code>ResultSet</code>
	 * object.
	 * 
	 * @return <code>true</code> if the cursor is on a valid row;
	 *         <code>false</code> if it is off the result set
	 * @exception SQLException
	 *                if a database access error occurs or the result set type
	 *                is <code>TYPE_FORWARD_ONLY</code>
	 */
	public boolean previous() throws SQLException {
		--iRecordNo;

		return readData();
	}

	/**
	 * Retrieves whether the cursor is before the first row in this
	 * <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is before the first row;
	 *         <code>false</code> if the cursor is at any other position or the
	 *         result set contains no rows
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public boolean isBeforeFirst() throws SQLException {
		return (getRecordNo() < FIRST_RECORD);
	}

	/**
	 * Retrieves whether the cursor is after the last row in this
	 * <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is after the last row;
	 *         <code>false</code> if the cursor is at any other position or the
	 *         result set contains no rows
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public boolean isAfterLast() throws SQLException {
		return (getRecordNo() >= alRecords.size());
	}

	/**
	 * Retrieves whether the cursor is on the first row of this
	 * <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is on the first row;
	 *         <code>false</code> otherwise
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public boolean isFirst() throws SQLException {
		return (getRecordNo() == FIRST_RECORD);
	}

	/**
	 * Retrieves whether the cursor is on the last row of this
	 * <code>ResultSet</code> object. Note: Calling the method
	 * <code>isLast</code> may be expensive because the JDBC driver might need
	 * to fetch ahead one row in order to determine whether the current row is
	 * the last row in the result set.
	 * 
	 * @return <code>true</code> if the cursor is on the last row;
	 *         <code>false</code> otherwise
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public boolean isLast() throws SQLException {
		return (getRecordNo() == (alRecords.size() - 1)); // as its 0 is
															// considered
	}

	/**
	 * Moves the cursor to the front of this <code>ResultSet</code> object, just
	 * before the first row. This method has no effect if the result set
	 * contains no rows.
	 * 
	 * @exception SQLException
	 *                if a database access error occurs or the result set type
	 *                is <code>TYPE_FORWARD_ONLY</code>
	 */
	public void beforeFirst() throws SQLException {
		iRecordNo = FIRST_RECORD - 1;
	}

	/**
	 * Moves the cursor to the end of this <code>ResultSet</code> object, just
	 * after the last row. This method has no effect if the result set contains
	 * no rows.
	 * 
	 * @exception SQLException
	 *                if a database access error occurs or the result set type
	 *                is <code>TYPE_FORWARD_ONLY</code>
	 */
	public void afterLast() throws SQLException {
		iRecordNo = alRecords.size();
	}

	/**
	 * Moves the cursor to the first row in this <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is on a valid row;
	 *         <code>false</code> if there are no rows in the result set
	 * @exception SQLException
	 *                if a database access error occurs or the result set type
	 *                is <code>TYPE_FORWARD_ONLY</code>
	 */
	public boolean first() throws SQLException {
		iRecordNo = FIRST_RECORD;

		return readData();
	}

	/**
	 * Moves the cursor to the last row in this <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is on a valid row;
	 *         <code>false</code> if there are no rows in the result set
	 * @exception SQLException
	 *                if a database access error occurs or the result set type
	 *                is <code>TYPE_FORWARD_ONLY</code>
	 */
	public boolean last() throws SQLException {
		iRecordNo = (alRecords.size() - 1);

		return readData();
	}

	/**
	 * Retrieves the current row number. The first row is number 1, the second
	 * number 2, and so on.
	 * 
	 * @return the current row number; <code>0</code> if there is no current row
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public int getRow() throws SQLException {
		return (((getRecordNo() < FIRST_RECORD) || (getRecordNo() >= alRecords
				.size())) ? 0 : (getRecordNo() + 1));
	}

	/**
	 * Moves the cursor to the given row number in this <code>ResultSet</code>
	 * object.
	 * 
	 * <p>
	 * If the row number is positive, the cursor moves to the given row number
	 * with respect to the beginning of the result set. The first row is row 1,
	 * the second is row 2, and so on.
	 * 
	 * <p>
	 * If the given row number is negative, the cursor moves to an absolute row
	 * position with respect to the end of the result set. For example, calling
	 * the method <code>absolute(-1)</code> positions the cursor on the last
	 * row; calling the method <code>absolute(-2)</code> moves the cursor to the
	 * next-to-last row, and so on.
	 * 
	 * <p>
	 * An attempt to position the cursor beyond the first/last row in the result
	 * set leaves the cursor before the first row or after the last row.
	 * 
	 * <p>
	 * <B>Note:</B> Calling <code>absolute(1)</code> is the same as calling
	 * <code>first()</code>. Calling <code>absolute(-1)</code> is the same as
	 * calling <code>last()</code>.
	 * 
	 * @param row
	 *            the number of the row to which the cursor should move. A
	 *            positive number indicates the row number counting from the
	 *            beginning of the result set; a negative number indicates the
	 *            row number counting from the end of the result set
	 * @return <code>true</code> if the cursor is on the result set;
	 *         <code>false</code> otherwise
	 * @exception SQLException
	 *                if a database access error occurs, or the result set type
	 *                is <code>TYPE_FORWARD_ONLY</code>
	 */
	public boolean absolute(int row) throws SQLException {
		if (row >= 0) {
			iRecordNo = row - 1;
		} else {
			iRecordNo = alRecords.size() + (row); // Note row is negative here
													// so it will be subtracted
		}

		return readData();
	}

	/**
	 * Moves the cursor a relative number of rows, either positive or negative.
	 * Attempting to move beyond the first/last row in the result set positions
	 * the cursor before/after the the first/last row. Calling
	 * <code>relative(0)</code> is valid, but does not change the cursor
	 * position.
	 * 
	 * <p>
	 * Note: Calling the method <code>relative(1)</code> is identical to calling
	 * the method <code>next()</code> and calling the method
	 * <code>relative(-1)</code> is identical to calling the method
	 * <code>previous()</code>.
	 * 
	 * @param rows
	 *            an <code>int</code> specifying the number of rows to move from
	 *            the current row; a positive number moves the cursor forward; a
	 *            negative number moves the cursor backward
	 * @return <code>true</code> if the cursor is on a row; <code>false</code>
	 *         otherwise
	 * @exception SQLException
	 *                if a database access error occurs, there is no current
	 *                row, or the result set type is
	 *                <code>TYPE_FORWARD_ONLY</code>
	 */
	public boolean relative(int rows) throws SQLException {
		iRecordNo = getRecordNo() + rows;

		return readData();
	}

	/*
	 * Utility Method to return and update the record No and takes into account
	 * AFTER LAST and BEFORE FIRST
	 */
	private int getRecordNo() {
		if (iRecordNo < FIRST_RECORD) {
			iRecordNo = FIRST_RECORD - 1;
		} else if (iRecordNo >= alRecords.size()) {
			iRecordNo = alRecords.size();
		}

		return iRecordNo;
	}

	private boolean readData() throws SQLException {
		fieldValues = new String[columnNames.length];

		if ((getRecordNo() < FIRST_RECORD)
				|| (getRecordNo() >= alRecords.size())) {
			return false;
		}

		fieldValues = (String[]) alRecords.get(iRecordNo);

		return true;
	}
}
