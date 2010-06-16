/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2001  Jonathan Ackerman

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.sql.Types;

import org.relique.io.CryptoFilter;
import org.relique.io.EncryptedFileOutputStream;
import org.relique.io.XORCipher;
import org.relique.jdbc.csv.CsvResultSet;

import junit.framework.TestCase;

/**
 * This class is used to test the CsvJdbc driver.
 * 
 * @author Jonathan Ackerman
 * @author JD Evora
 * @author Chetan Gupta
 * @author Mario Frasca
 * @version $Id: TestCryptoFilter.java,v 1.1 2010/06/16 11:14:04 mfrasca Exp $
 */
public class TestCryptoFilter extends TestCase {
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;

	public TestCryptoFilter(String name) {
		super(name);
	}

	protected void setUp() {
		filePath = System.getProperty(SAMPLE_FILES_LOCATION_PROPERTY);
		if (filePath == null)
			filePath = RunTests.DEFAULT_FILEPATH;
		assertNotNull("Sample files location property not set !", filePath);

		// load CSV driver
		try {
			Class.forName("org.relique.jdbc.csv.CsvDriver");
		} catch (ClassNotFoundException e) {
			fail("Driver is not in the CLASSPATH -> " + e);
		}

	}

	/**
	 * using a wrong codec will cause an exception.
	 * @throws SQLException
	 */
	public void testCryptoFilterNoCodec() throws SQLException {
		Properties props = new Properties();
		props.put("cryptoFilterClassName", "org.relique.io.NotACodec");
		props.put("cryptoFilterParameterTypes", "String");
		props.put("cryptoFilterParameters", "@0y");
		try {
			DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			fail("managed to initialize not existing CryptoFilter");
		} catch (SQLException e) {
			assertEquals(
					"java.sql.SQLException: could not find codec class org.relique.io.NotACodec",
					"" + e);
		}
	}

	/**
	 * 
	 * @throws SQLException
	 */
	public void testCryptoFilter() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("cryptoFilterClassName", "org.relique.io.XORCipher");
		props.put("cryptoFilterParameterTypes", "String");
		props.put("cryptoFilterParameters", "gaius vipsanius agrippa");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM scrambled");
		assertTrue(results.next());
		assertEquals("The key is wrong", "1", results.getString("key"));
		assertEquals("The value is wrong", "uno", results.getString("value"));
		assertTrue(results.next());
		assertEquals("The key is wrong", "2", results.getString("key"));
		assertEquals("The value is wrong", "due", results.getString("value"));
		assertTrue(results.next());
		assertEquals("The key is wrong", "3", results.getString("key"));
		assertEquals("The value is wrong", "tre", results.getString("value"));
		assertTrue(!results.next());
	}
	
	/**
	 * wrong key: behave as if the file was empty.
	 * @throws SQLException
	 */
	public void testCryptoFilterWrongKey() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("cryptoFilterClassName", "org.relique.io.XORCipher");
		props.put("cryptoFilterParameterTypes", "String");
		props.put("cryptoFilterParameters", "marcus junius brutus");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM scrambled");
		assertFalse(results.next());		
	}

	/**
	 * file with empty lines
	 * @throws SQLException
	 */
	public void testCryptoFilterWithEmptyLines() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("cryptoFilterClassName", "org.relique.io.XORCipher");
		props.put("cryptoFilterParameterTypes", "String");
		props.put("cryptoFilterParameters", "gaius vipsanius agrippa");
		props.put("ignoreNonParseableLines", "True");

		boolean generating_input_file = false;
		if(generating_input_file  ) {
			try {
				File f = new File(filePath, "scrambled_trailing.txt");
				String filename = f.getAbsolutePath();
				CryptoFilter cip = new XORCipher(new String("gaius vipsanius agrippa"));;
				EncryptedFileOutputStream out = new EncryptedFileOutputStream(filename, cip );
				out.write((new String("key,value\n")).getBytes());
				out.write((new String("1,uno\n")).getBytes());
				out.write((new String("\n")).getBytes());
				out.write((new String("2,due\n")).getBytes());
				out.write((new String("3,tre\n")).getBytes());
				out.write((new String("\n")).getBytes());
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM scrambled_trailing");
		assertTrue(results.next());
		assertEquals("The key is wrong", "1", results.getString("key"));
		assertEquals("The value is wrong", "uno", results.getString("value"));
		assertTrue(results.next());
		assertEquals("The key is wrong", "2", results.getString("key"));
		assertEquals("The value is wrong", "due", results.getString("value"));
		assertTrue(results.next());
		assertEquals("The key is wrong", "3", results.getString("key"));
		assertEquals("The value is wrong", "tre", results.getString("value"));
		assertTrue(!results.next());
	}
	
	public void testSecondQuery() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("cryptoFilterClassName", "org.relique.io.XORCipher");
		props.put("cryptoFilterParameterTypes", "String");
		props.put("cryptoFilterParameters", "gaius vipsanius agrippa");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM scrambled");
		assertTrue(results.next());
		assertEquals("The key is wrong", "1", results.getString("key"));
		assertEquals("The value is wrong", "uno", results.getString("value"));
		assertTrue(results.next());
		assertEquals("The key is wrong", "2", results.getString("key"));
		assertEquals("The value is wrong", "due", results.getString("value"));

		results = stmt.executeQuery("SELECT * FROM scrambled");
		assertTrue(results.next());
		assertEquals("The key is wrong", "1", results.getString("key"));
		assertEquals("The value is wrong", "uno", results.getString("value"));
		assertTrue(results.next());
		assertEquals("The key is wrong", "2", results.getString("key"));
		assertEquals("The value is wrong", "due", results.getString("value"));
		assertTrue(results.next());
		assertEquals("The key is wrong", "3", results.getString("key"));
		assertEquals("The value is wrong", "tre", results.getString("value"));
		assertTrue(!results.next());
	}
	
}