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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Calendar;
import java.sql.*;

/**
 * This class implements the ResultSet interface for the CsvJdbc driver.
 *
 * @author     Jonathan Ackerman
 * @author     Michael Maraya
 * @version    $Id: CsvResultSet.java,v 1.8 2002/08/18 20:31:49 mmaraya Exp $
 */
public class CsvResultSet implements ResultSet {

    /** Metadata for this ResultSet */
    protected ResultSetMetaData resultSetMetaData;

    /** Statement that produced this ResultSet */
    protected CsvStatement statement;

    /** Helper class that performs the actual file reads */
    protected CsvReader reader;

    /** Table referenced by the Statement */
    protected String tableName;

    /** Array of available columns for referenced table */
    protected String[] columnNames;

    /** Last column name index read */
    protected int lastIndexRead = -1;

    /** InputStream to keep track of */
    protected InputStream is;

    /**
     * Constructor for the CsvResultSet object
     *
     * @param statement Statement that produced this ResultSet
     * @param reader Helper class that performs the actual file reads
     * @param tableName Table referenced by the Statement
     * @param columnNames Array of available columns for referenced table
     */
    protected CsvResultSet(CsvStatement statement, CsvReader reader,
                           String tableName, String[] columnNames) {
        this.statement = statement;
        this.reader = reader;
        this.tableName = tableName;
        this.columnNames = columnNames;
        if(columnNames[0].equals("*")) {
            this.columnNames = reader.getColumnNames();
        }
    }

    /**
     * Moves the cursor down one row from its current position.
     * A <code>ResultSet</code> cursor is initially positioned
     * before the first row; the first call to the method
     * <code>next</code> makes the first row the current row; the
     * second call makes the second row the current row, and so on.
     *
     * <P>If an input stream is open for the current row, a call
     * to the method <code>next</code> will
     * implicitly close it. A <code>ResultSet</code> object's
     * warning chain is cleared when a new row is read.
     *
     * @return <code>true</code> if the new current row is valid;
     * <code>false</code> if there are no more rows
     * @exception SQLException if a database access error occurs
     */
    public boolean next() throws SQLException {
        return reader.next();
    }

    /**
     * Releases this <code>ResultSet</code> object's database and
     * JDBC resources immediately instead of waiting for
     * this to happen when it is automatically closed.
     *
     * <P><B>Note:</B> A <code>ResultSet</code> object
     * is automatically closed by the
     * <code>Statement</code> object that generated it when
     * that <code>Statement</code> object is closed,
     * re-executed, or is used to retrieve the next result from a
     * sequence of multiple results. A <code>ResultSet</code> object
     * is also automatically closed when it is garbage collected.
     *
     * @exception SQLException if a database access error occurs
     */
    public void close() throws SQLException {
        reader.close();
    }

    /**
     * Reports whether
     * the last column read had a value of SQL <code>NULL</code>.
     * Note that you must first call one of the getter methods
     * on a column to try to read its value and then call
     * the method <code>wasNull</code> to see if the value read was
     * SQL <code>NULL</code>.
     *
     * @return <code>true</code> if the last column value read was SQL
     *         <code>NULL</code> and <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean wasNull() throws SQLException {
        if(lastIndexRead >= 0) {
            return getString(lastIndexRead).equals(null);
        } else {
            throw new SQLException("No previous getter method called");
        }
    }

    //======================================================================
    // Methods for accessing results by column index
    //======================================================================

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>String</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public String getString(int columnIndex) throws SQLException {
        // perform pre-accessor method processing
        preAccessor(columnIndex);
        // use CsvReader.getColumn(String) to retrieve the column
        return reader.getColumn(columnNames[columnIndex-1]);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>boolean</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>false</code>
     * @exception SQLException if a database access error occurs
     */
    public boolean getBoolean(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? false : Boolean.valueOf(str).booleanValue();
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>byte</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public byte getByte(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? 0 : Byte.parseByte(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>short</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public short getShort(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? 0 : Short.parseShort(str);
    }

    /**
     * Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>int</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public int getInt(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? 0 : Integer.parseInt(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>long</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public long getLong(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? 0L : Long.parseLong(str);
    }

    /**
     * Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>float</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public float getFloat(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? 0F : Float.parseFloat(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>double</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public double getDouble(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? 0D : Double.parseDouble(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.BigDecimal</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param scale the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     * @deprecated
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? null : new BigDecimal(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>byte</code> array in the Java programming language.
     * The bytes represent the raw values returned by the driver.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? null : str.getBytes();
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Date</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public Date getDate(int columnIndex) throws SQLException  {
        String str = getString(columnIndex);
        return (str == null) ? null : Date.valueOf(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Time</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public Time getTime(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? null : Time.valueOf(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Timestamp</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? null : Timestamp.valueOf(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a stream of ASCII characters.
     * The value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large <char>LONGVARCHAR</char>
     * values. The JDBC driver will do any necessary conversion from the
     * database format into ASCII.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream.  Also, a
     * stream may return <code>0</code> when the method
     * <code>InputStream.available</code>
     * is called whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value
     * as a stream of one-byte ASCII characters;
     * if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        is = new ByteArrayInputStream(str.getBytes());
        return (str == null) ? null : is;
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * as a stream of two-byte Unicode characters. The first byte is
     * the high byte; the second byte is the low byte.
     *
     * The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARCHAR</code>values.  The
     * JDBC driver will do any necessary conversion from the database
     * format into Unicode.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream.
     * Also, a stream may return <code>0</code> when the method
     * <code>InputStream.available</code>
     * is called, whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value
     *         as a stream of two-byte Unicode characters;
     *         if the value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code>
     *
     * @exception SQLException if a database access error occurs
     * @deprecated use <code>getCharacterStream</code> in place of
     *              <code>getUnicodeStream</code>
     */
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        // delegate to getAsciiStream(int)
        return getAsciiStream(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a binary stream of
     * uninterpreted bytes. The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARBINARY</code> values.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream.  Also, a
     * stream may return <code>0</code> when the method
     * <code>InputStream.available</code>
     * is called whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value
     *         as a stream of uninterpreted bytes;
     *         if the value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        // delegate to getAsciiStream(int)
        return getAsciiStream(columnIndex);
    }

    //======================================================================
    // Methods for accessing results by column name
    //======================================================================

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>String</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public String getString(String columnName) throws SQLException {
        // perform pre-accessor method processing
        preAccessor(columnName);
        // use CsvReader.getColumn(String) to retrieve the column
        return reader.getColumn(columnName);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>boolean</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>false</code>
     * @exception SQLException if a database access error occurs
     */
    public boolean getBoolean(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? false : Boolean.valueOf(str).booleanValue();
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>byte</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public byte getByte(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? 0 : Byte.parseByte(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>short</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public short getShort(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? 0 : Short.parseShort(str);
    }

    /**
     * Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>int</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public int getInt(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? 0 : Integer.parseInt(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>long</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public long getLong(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? 0L : Long.parseLong(str);
    }

    /**
     * Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>float</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public float getFloat(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? 0F : Float.parseFloat(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>double</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @exception SQLException if a database access error occurs
     */
    public double getDouble(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? 0D : Double.parseDouble(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.math.BigDecimal</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @param scale the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     * @deprecated
     */
    public BigDecimal getBigDecimal(String columnName, int scale)
            throws SQLException {
        String str = getString(columnName);
        return (str == null) ? null : new BigDecimal(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>byte</code> array in the Java programming language.
     * The bytes represent the raw values returned by the driver.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public byte[] getBytes(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? null : str.getBytes();
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Date</code> object in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public Date getDate(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? null : Date.valueOf(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Time</code> object in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public Time getTime(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? null : Time.valueOf(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Timestamp</code> object.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public Timestamp getTimestamp(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? null : Timestamp.valueOf(str);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a stream of
     * ASCII characters. The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARCHAR</code> values.
     * The JDBC driver will
     * do any necessary conversion from the database format into ASCII.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream. Also, a
     * stream may return <code>0</code> when the method <code>available</code>
     * is called whether there is data available or not.
     *
     * @param columnName the SQL name of the column
     * @return a Java input stream that delivers the database column value
     * as a stream of one-byte ASCII characters.
     * If the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code>.
     * @exception SQLException if a database access error occurs
     */
    public InputStream getAsciiStream(String columnName) throws SQLException {
        String str = getString(columnName);
        is = new ByteArrayInputStream(str.getBytes());
        return (str == null) ? null : is;
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a stream of two-byte
     * Unicode characters. The first byte is the high byte; the second
     * byte is the low byte.
     *
     * The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARCHAR</code> values.
     * The JDBC technology-enabled driver will
     * do any necessary conversion from the database format into Unicode.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream.
     * Also, a stream may return <code>0</code> when the method
     * <code>InputStream.available</code> is called, whether there
     * is data available or not.
     *
     * @param columnName the SQL name of the column
     * @return a Java input stream that delivers the database column value
     *         as a stream of two-byte Unicode characters.
     *         If the value is SQL <code>NULL</code>, the value returned
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @deprecated use <code>getCharacterStream</code> instead
     */
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        // delegate to getAsciiStream(String)
        return getAsciiStream(columnName);
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a stream of uninterpreted
     * <code>byte</code>s.
     * The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARBINARY</code>
     * values.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream. Also, a
     * stream may return <code>0</code> when the method <code>available</code>
     * is called whether there is data available or not.
     *
     * @param columnName the SQL name of the column
     * @return a Java input stream that delivers the database column value
     * as a stream of uninterpreted bytes;
     * if the value is SQL <code>NULL</code>, the result is <code>null</code>
     * @exception SQLException if a database access error occurs
     */
    public InputStream getBinaryStream(String columnName) throws SQLException {
        // delegate to getAsciiStream(String)
        return getAsciiStream(columnName);
    }

    //=====================================================================
    // Advanced features:
    //=====================================================================

    /**
     * Retrieves the first warning reported by calls on this
     * <code>ResultSet</code> object.
     * Subsequent warnings on this <code>ResultSet</code> object
     * will be chained to the <code>SQLWarning</code> object that
     * this method returns.
     *
     * <P>The warning chain is automatically cleared each time a new
     * row is read.  This method may not be called on a <code>ResultSet</code>
     * object that has been closed; doing so will cause an
     * <code>SQLException</code> to be thrown.
     * <P>
     * <B>Note:</B> This warning chain only covers warnings caused
     * by <code>ResultSet</code> methods.  Any warning caused by
     * <code>Statement</code> methods
     * (such as reading OUT parameters) will be chained on the
     * <code>Statement</code> object.
     *
     * @return the first <code>SQLWarning</code> object reported or
     *         <code>null</code> if there are none
     * @exception SQLException if a database access error occurs or this method is
     *            called on a closed result set
     */
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getWarnings() unsupported");
    }

    /**
     * Clears all warnings reported on this <code>ResultSet</code> object.
     * After this method is called, the method <code>getWarnings</code>
     * returns <code>null</code> until a new warning is
     * reported for this <code>ResultSet</code> object.
     *
     * @exception SQLException if a database access error occurs
     */
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.clearWarnings() unsupported");
    }

    /**
     * Retrieves the name of the SQL cursor used by this <code>ResultSet</code>
     * object.
     *
     * <P>In SQL, a result table is retrieved through a cursor that is
     * named. The current row of a result set can be updated or deleted
     * using a positioned update/delete statement that references the
     * cursor name. To insure that the cursor has the proper isolation
     * level to support update, the cursor's <code>SELECT</code> statement
     * should be of the form <code>SELECT FOR UPDATE</code>. If
     * <code>FOR UPDATE</code> is omitted, the positioned updates may fail.
     *
     * <P>The JDBC API supports this SQL feature by providing the name of the
     * SQL cursor used by a <code>ResultSet</code> object.
     * The current row of a <code>ResultSet</code> object
     * is also the current row of this SQL cursor.
     *
     * <P><B>Note:</B> If positioned update is not supported, a
     * <code>SQLException</code> is thrown.
     *
     * @return the SQL name for this <code>ResultSet</code> object's cursor
     * @exception SQLException if a database access error occurs
     */
    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getCursorName() unsupported");
    }

    /**
     * Retrieves the  number, types and properties of
     * this <code>ResultSet</code> object's columns.
     *
     * @return the description of this <code>ResultSet</code> object's columns
     * @exception SQLException if a database access error occurs
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        if (resultSetMetaData == null) {
            resultSetMetaData = new CsvResultSetMetaData(tableName,columnNames);
        }
        return resultSetMetaData;
    }

    /**
     * <p>Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>Object</code> in the Java programming language.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java object type corresponding to the column's SQL type,
     * following the mapping for built-in types specified in the JDBC
     * specification. If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     *
     * <p>This method may also be used to read datatabase-specific
     * abstract data types.
     *
     * In the JDBC 2.0 API, the behavior of method
     * <code>getObject</code> is extended to materialize
     * data of SQL user-defined types.  When a column contains
     * a structured or distinct value, the behavior of this method is as
     * if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.lang.Object</code> holding the column value
     * @exception SQLException if a database access error occurs
     */
    public Object getObject(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getObject(int) unsupported");
    }

    /**
     * <p>Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>Object</code> in the Java programming language.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java object type corresponding to the column's SQL type,
     * following the mapping for built-in types specified in the JDBC
     * specification. If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     * <P>
     * This method may also be used to read datatabase-specific
     * abstract data types.
     * <P>
     * In the JDBC 2.0 API, the behavior of the method
     * <code>getObject</code> is extended to materialize
     * data of SQL user-defined types.  When a column contains
     * a structured or distinct value, the behavior of this method is as
     * if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     *
     * @param columnName the SQL name of the column
     * @return a <code>java.lang.Object</code> holding the column value
     * @exception SQLException if a database access error occurs
     */
    public Object getObject(String columnName) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getObject(String) unsupported");
    }

    /**
     * Maps the given <code>ResultSet</code> column name to its
     * <code>ResultSet</code> column index.
     *
     * @param columnName the name of the column
     * @return the column index of the given column name
     * @exception SQLException if the <code>ResultSet</code> object
     * does not contain <code>columnName</code> or a database access error occurs
     */
    public int findColumn(String columnName) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.findColumn(String) unsupported");
    }

    //--------------------------JDBC 2.0-----------------------------------

    /**
     * Gives a hint as to the direction in which the rows in this
     * <code>ResultSet</code> object will be processed. The initial value is
     * determined by the <code>Statement</code> object that produced this
     * <code>ResultSet</code> object. The fetch direction may be changed at
     * any time.
     *
     * @param direction an <code>int</code> specifying the suggested
     *        fetch direction; one of <code>ResultSet.FETCH_FORWARD</code>,
     *        <code>ResultSet.FETCH_REVERSE</code>, or
     *        <code>ResultSet.FETCH_UNKNOWN</code>
     * @exception SQLException if a database access error occurs or
     *            the result set type is <code>TYPE_FORWARD_ONLY</code>
     *            and the fetch direction is not <code>FETCH_FORWARD</code>
     */
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.setFetchDirection(int) unsupported");
    }

    /**
     * Gives the JDBC driver a hint as to the number of rows that should
     * be fetched from the database when more rows are needed for this
     * <code>ResultSet</code> object. If the fetch size specified is zero,
     * the JDBC driver ignores the value and is free to make its own best
     * guess as to what the fetch size should be.  The default value is set
     * by the <code>Statement</code> object that created the result set.
     * The fetch size may be changed at any time.
     *
     * @param rows the number of rows to fetch
     * @exception SQLException if a database access error occurs or the
     * condition <code>0 <= rows <= this.getMaxRows()</code> is not satisfied
     */
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.setFetchSize(int) unsupported");
    }

    /**
     * Retrieves the <code>Statement</code> object that produced this
     * <code>ResultSet</code> object.
     * If the result set was generated some other way, such as by a
     * <code>DatabaseMetaData</code> method, this method returns
     * <code>null</code>.
     *
     * @return the <code>Statment</code> object that produced
     * this <code>ResultSet</code> object or <code>null</code>
     * if the result set was produced some other way
     * @exception SQLException if a database access error occurs
     */
    public Statement getStatement() throws SQLException {
        return statement;
    }


    /**
     *Gets the characterStream attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The characterStream value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Reader getCharacterStream(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.getCharacterStream(int) Not Supported !");
    }


    /**
     *Gets the characterStream attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The characterStream value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Reader getCharacterStream(String p0) throws SQLException
    {
        throw new SQLException("ResultSet.getCharacterStream(String) Not Supported !");
    }


    /**
     *Gets the bigDecimal attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The bigDecimal value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public BigDecimal getBigDecimal(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.getBigDecimal(int) Not Supported !");
    }


    /**
     *Gets the bigDecimal attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The bigDecimal value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public BigDecimal getBigDecimal(String p0) throws SQLException
    {
        throw new SQLException("ResultSet.getBigDecimal(String) Not Supported !");
    }


    /**
     *Gets the beforeFirst attribute of the CsvResultSet object
     *
     * @return                   The beforeFirst value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean isBeforeFirst() throws SQLException
    {
        throw new SQLException("ResultSet.isBeforeFirst() Not Supported !");
    }


    /**
     *Gets the afterLast attribute of the CsvResultSet object
     *
     * @return                   The afterLast value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean isAfterLast() throws SQLException
    {
        throw new SQLException("ResultSet.isAfterLast() Not Supported !");
    }


    /**
     *Gets the first attribute of the CsvResultSet object
     *
     * @return                   The first value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean isFirst() throws SQLException
    {
        throw new SQLException("ResultSet.isFirst() Not Supported !");
    }


    /**
     *Gets the last attribute of the CsvResultSet object
     *
     * @return                   The last value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean isLast() throws SQLException
    {
        throw new SQLException("ResultSet.isLast() Not Supported !");
    }


    /**
     *Gets the row attribute of the CsvResultSet object
     *
     * @return                   The row value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getRow() throws SQLException
    {
        throw new SQLException("ResultSet.getRow() Not Supported !");
    }


    /**
     *Gets the fetchDirection attribute of the CsvResultSet object
     *
     * @return                   The fetchDirection value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getFetchDirection() throws SQLException
    {
        throw new SQLException("ResultSet.getFetchDirection() Not Supported !");
    }


    /**
     *Gets the fetchSize attribute of the CsvResultSet object
     *
     * @return                   The fetchSize value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getFetchSize() throws SQLException
    {
        throw new SQLException("ResultSet.getFetchSize() Not Supported !");
    }


    /**
     *Gets the type attribute of the CsvResultSet object
     *
     * @return                   The type value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getType() throws SQLException
    {
        throw new SQLException("ResultSet.getType() Not Supported !");
    }


    /**
     *Gets the concurrency attribute of the CsvResultSet object
     *
     * @return                   The concurrency value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getConcurrency() throws SQLException
    {
        throw new SQLException("ResultSet.getConcurrency() Not Supported !");
    }


    /**
     *Gets the object attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @return                   The object value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Object getObject(int p0, Map p1) throws SQLException
    {
        throw new SQLException("ResultSet.getObject(int,Map) Not Supported !");
    }


    /**
     *Gets the ref attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The ref value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Ref getRef(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.getRef(int) Not Supported !");
    }


    /**
     *Gets the blob attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The blob value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Blob getBlob(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.getBlob(int) Not Supported !");
    }


    /**
     *Gets the clob attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The clob value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Clob getClob(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.getClob(int) Not Supported !");
    }


    /**
     *Gets the array attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The array value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Array getArray(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.getArray(int) Not Supported !");
    }


    /**
     *Gets the object attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @return                   The object value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Object getObject(String p0, Map p1) throws SQLException
    {
        throw new SQLException("ResultSet.getObject(String, Map) Not Supported !");
    }


    /**
     *Gets the ref attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The ref value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Ref getRef(String p0) throws SQLException
    {
        throw new SQLException("ResultSet.getRef(String) Not Supported !");
    }


    /**
     *Gets the blob attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The blob value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Blob getBlob(String p0) throws SQLException
    {
        throw new SQLException("ResultSet.getBlob(String) Not Supported !");
    }


    /**
     *Gets the clob attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The clob value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Clob getClob(String p0) throws SQLException
    {
        throw new SQLException("ResultSet.getClob(String) Not Supported !");
    }


    /**
     *Gets the array attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @return                   The array value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Array getArray(String p0) throws SQLException
    {
        throw new SQLException("ResultSet.getArray(String) Not Supported !");
    }


    /**
     *Gets the date attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @return                   The date value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Date getDate(int p0, Calendar p1) throws SQLException
    {
        throw new SQLException("ResultSet.getDate(int, Calendar) Not Supported !");
    }


    /**
     *Gets the date attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @return                   The date value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Date getDate(String p0, Calendar p1) throws SQLException
    {
        throw new SQLException("ResultSet.getDate(String, Calendar) Not Supported !");
    }


    /**
     *Gets the time attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @return                   The time value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Time getTime(int p0, Calendar p1) throws SQLException
    {
        throw new SQLException("ResultSet.getTime(int, Calendar) Not Supported !");
    }


    /**
     *Gets the time attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @return                   The time value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Time getTime(String p0, Calendar p1) throws SQLException
    {
        throw new SQLException("ResultSet.getTime(String, Calendar) Not Supported !");
    }


    /**
     *Gets the timestamp attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @return                   The timestamp value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Timestamp getTimestamp(int p0, Calendar p1) throws SQLException
    {
        throw new SQLException("ResultSet.getTimestamp(int, Calendar) Not Supported !");
    }


    /**
     *Gets the timestamp attribute of the CsvResultSet object
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @return                   The timestamp value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Timestamp getTimestamp(String p0, Calendar p1) throws SQLException
    {
        throw new SQLException("ResultSet.getTimestamp(String, Calendar) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void beforeFirst() throws SQLException
    {
        throw new SQLException("ResultSet.beforeFirst() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void afterLast() throws SQLException
    {
        throw new SQLException("ResultSet.afterLast() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean first() throws SQLException
    {
        throw new SQLException("ResultSet.first() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean last() throws SQLException
    {
        throw new SQLException("ResultSet.last() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean absolute(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.absolute(int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean relative(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.relative(int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean previous() throws SQLException
    {
        throw new SQLException("ResultSet.previous() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean rowUpdated() throws SQLException
    {
        throw new SQLException("ResultSet.rowUpdated() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean rowInserted() throws SQLException
    {
        throw new SQLException("ResultSet.rowInserted() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean rowDeleted() throws SQLException
    {
        throw new SQLException("ResultSet.rowDeleted() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateNull(int p0) throws SQLException
    {
        throw new SQLException("ResultSet.updatedNull(int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateBoolean(int p0, boolean p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateBoolean(int, boolean) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateByte(int p0, byte p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateByte(int, byte) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateShort(int p0, short p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateShort(int, short) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateInt(int p0, int p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateInt(int, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateLong(int p0, long p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateLong(int, long) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateFloat(int p0, float p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateFloat(int, float) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateDouble(int p0, double p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateDouble(int, double) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateBigDecimal(int p0, BigDecimal p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateBigDecimal(int, BigDecimal) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateString(int p0, String p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateString(int, String) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateBytes(int p0, byte[] p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateBytes(int, byte[]) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateDate(int p0, Date p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateDate(int, Date) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateTime(int p0, Time p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateTime(int, Time) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateTimestamp(int p0, Timestamp p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateTimestamp(int, Timestamp) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @param  p2                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateAsciiStream(int p0, InputStream p1, int p2) throws SQLException
    {
        throw new SQLException("ResultSet.updateAsciiStream(int, InputStream, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @param  p2                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateBinaryStream(int p0, InputStream p1, int p2) throws SQLException
    {
        throw new SQLException("ResultSet.updateBinaryStream(int, InputStream, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @param  p2                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateCharacterStream(int p0, Reader p1, int p2) throws SQLException
    {
        throw new SQLException("ResultSet.updateCharacterStream(int, Reader, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @param  p2                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateObject(int p0, Object p1, int p2) throws SQLException
    {
        throw new SQLException("ResultSet.updateObject(int, Object, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateObject(int p0, Object p1) throws SQLException
    {
        throw new SQLException("ResultSet.udpateObject(int, Object) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateNull(String p0) throws SQLException
    {
        throw new SQLException("ResultSet.updateNull(String) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateBoolean(String p0, boolean p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateBoolean(String, boolean) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateByte(String p0, byte p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateByte(String, byte) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateShort(String p0, short p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateShort(String, short) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateInt(String p0, int p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateInt(String, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateLong(String p0, long p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateLong(String, long) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateFloat(String p0, float p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateFloat(String, float) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateDouble(String p0, double p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateDouble(String, double) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateBigDecimal(String p0, BigDecimal p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateBigDecimal(String, BigDecimal) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateString(String p0, String p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateString(String, String) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateBytes(String p0, byte[] p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateBytes(String, byte[]) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateDate(String p0, Date p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateDate(String, Date) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateTime(String p0, Time p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateTime(String, Time) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateTimestamp(String p0, Timestamp p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateTimestamp(String, Timestamp) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @param  p2                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateAsciiStream(String p0, InputStream p1, int p2) throws SQLException
    {
        throw new SQLException("ResultSet.updateAsciiStream(String, InputStream, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @param  p2                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateBinaryStream(String p0, InputStream p1, int p2) throws SQLException
    {
        throw new SQLException("ResultSet.updateBinaryStream(String, InputStream, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @param  p2                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateCharacterStream(String p0, Reader p1, int p2) throws SQLException
    {
        throw new SQLException("ResultSet.updateCharacterStream(String, Reader, int)Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @param  p2                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateObject(String p0, Object p1, int p2) throws SQLException
    {
        throw new SQLException("ResultSet.updateObject(String, Object, int) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @param  p0                Description of Parameter
     * @param  p1                Description of Parameter
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateObject(String p0, Object p1) throws SQLException
    {
        throw new SQLException("ResultSet.updateObject(String, Object) Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void insertRow() throws SQLException
    {
        throw new SQLException("ResultSet.insertRow() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void updateRow() throws SQLException
    {
        throw new SQLException("ResultSet.update() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void deleteRow() throws SQLException
    {
        throw new SQLException("ResultSet.deleteRow() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void refreshRow() throws SQLException
    {
        throw new SQLException("ResultSet.refreshRow() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void cancelRowUpdates() throws SQLException
    {
        throw new SQLException("ResultSet.cancelRowUpdates() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void moveToInsertRow() throws SQLException
    {
        throw new SQLException("ResultSet.moveToInsertRow() Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void moveToCurrentRow() throws SQLException
    {
        throw new SQLException("ResultSet.moveToeCurrentRow() Not Supported !");
    }

    /**
     * Perform pre-accessor method processing
     * @param columnIndex the first column is 1, the second is 2, ...
     * @exception SQLException if a database access error occurs
     */
    private void preAccessor(int columnIndex) throws SQLException {
        // set last read column index for wasNull()
        lastIndexRead = columnIndex;
        // implicitly close InputStream for get*Stream() between accessors
        if(is != null) {
            try {
                is.close();
            } catch (IOException e) {
                throw new SQLException("Could not close InputStream: " + e);
            }
            is = null;
        }
    }

    /**
     * Perform pre-accessor method processing
     * @param columnName the SQL name of the column
     * @exception SQLException if a database access error occurs
     */
    private void preAccessor(String columnName) throws SQLException {
        // locate the index number and delegate to preAccessor(int)
        for (int i = 0; i < columnNames.length; i++) {
            if (columnName.equalsIgnoreCase(columnNames[i])) {
                preAccessor(i+1);
            }
        }
    }

}

