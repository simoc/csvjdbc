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
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.relique.io.CryptoFilter;
import org.relique.io.EncryptedFileOutputStream;
import org.relique.io.XORCipher;

/**
 * This class is used to test the CsvJdbc driver.
 * 
 * @author Jonathan Ackerman
 * @author JD Evora
 * @author Chetan Gupta
 * @author Mario Frasca
 */
public class TestCryptoFilter
{
	private static String filePath;
	private static int testSize = 1100;

	@BeforeClass
	public static void setUp()
	{
		filePath = ".." + File.separator + "src" + File.separator + "testdata";
		if (!new File(filePath).isDirectory())
			filePath = "src" + File.separator + "testdata";
		assertTrue("Sample files directory not found: " + filePath, new File(filePath).isDirectory());

		// load CSV driver
		try
		{
			Class.forName("org.relique.jdbc.csv.CsvDriver");
		}
		catch (ClassNotFoundException e)
		{
			fail("Driver is not in the CLASSPATH -> " + e);
		}

		// Copy a scrambles test file many times
		String source = filePath + System.getProperty("file.separator")
				+ "scrambled.txt";
		int sampleLength = (int) new File(source).length();
		try
		{
			for (int i = 0; i < testSize; i++)
			{
				String destination = filePath
						+ System.getProperty("file.separator") + "scrambled_"
						+ i + ".txt";
				FileInputStream fis;
				fis = new FileInputStream(source);
				BufferedInputStream in = new BufferedInputStream(fis);
				FileOutputStream fos = new FileOutputStream(destination);
				BufferedOutputStream out = new BufferedOutputStream(fos);
				byte[] buffer = new byte[sampleLength];
				int len = 0;
				while ((len = in.read(buffer)) >= 0)
				{
					out.write(buffer, 0, len);
				}
				in.close();
				fis.close();
				out.close();
				fos.close();
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@AfterClass
	public static void tearDown()
	{
		// and delete the files when ready
		for (int i = 0; i < testSize; i++)
		{
			String testFile = filePath + System.getProperty("file.separator")
					+ "scrambled_" + i + ".txt";
			File file = new File(testFile);
			assertTrue(file.delete());
		}
	}	

	/**
	 * using a wrong codec will cause an exception.
	 * @throws SQLException
	 */
	@Test
	public void testCryptoFilterNoCodec() throws SQLException
	{
		Properties props = new Properties();
		props.put("cryptoFilterClassName", "org.relique.io.NotACodec");
		props.put("cryptoFilterParameterTypes", "String");
		props.put("cryptoFilterParameters", "@0y");
		try
		{
			DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			fail("managed to initialize not existing CryptoFilter");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " +
				CsvResources.getString("noCodecClass") + ": org.relique.io.NotACodec", "" + e);
		}
	}

	@Test
	public void testCryptoFilter() throws SQLException
	{
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
	@Test
	public void testCryptoFilterWrongKey() throws SQLException
	{
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
	@Test
	public void testCryptoFilterWithEmptyLines() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("cryptoFilterClassName", "org.relique.io.XORCipher");
		props.put("cryptoFilterParameterTypes", "String");
		props.put("cryptoFilterParameters", "gaius vipsanius agrippa");
		props.put("ignoreNonParseableLines", "True");

		boolean generating_input_file = false;
		if(generating_input_file)
		{
			EncryptedFileOutputStream out = null;
			try
			{
				File f = new File(filePath, "scrambled_trailing.txt");
				String filename = f.getAbsolutePath();
				CryptoFilter cip = new XORCipher(new String("gaius vipsanius agrippa"));;
				out = new EncryptedFileOutputStream(filename, cip );
				out.write((new String("key,value\n")).getBytes());
				out.write((new String("1,uno\n")).getBytes());
				out.write((new String("\n")).getBytes());
				out.write((new String("2,due\n")).getBytes());
				out.write((new String("3,tre\n")).getBytes());
				out.write((new String("\n")).getBytes());
			}
			catch (FileNotFoundException e)
			{
				e.printStackTrace();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				try
				{
					if (out != null)
						out.close();
				}
				catch (IOException e)
				{
				}
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

	@Test
	public void testSecondQuery() throws SQLException
	{
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

	@Test
	public void testScrambledFileSpeed() throws SQLException
	{
		// not so sure why we need this test... timings are so terribly system
		// dependent, this test will cause random regressions just because the
		// load on the server performing the test might happen to be heavier
		// during one function and lighter during the other!
		
		// creating variables - to be initialized later.
		Properties props = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rset = null;

		long timeNoEncrypt = 0;
		long timeEncrypt = 0;

		int encryptCount = 0;
		int noEncryptCount = 0;

		// Run queries twice to reduce the chances of system load causing the test to fail.
		for (int i = 0; i < 2; i++)
		{
			// encrypted
			props = new Properties();
			props.put("fileExtension", ".txt");
			props.put("cryptoFilterClassName", "org.relique.io.XORCipher");
			props.put("cryptoFilterParameterTypes", "String");
			props.put("cryptoFilterParameters", "gaius vipsanius agrippa");
			Connection connEncr = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);
			stmt = connEncr.createStatement();
			long encryptStartMillis = System.currentTimeMillis();
			rset = stmt.executeQuery("SELECT * FROM speedtest_decypher");
			while (rset.next())
				encryptCount++;
			connEncr.close();
			long encryptEndMillis = System.currentTimeMillis();

			timeEncrypt += encryptEndMillis - encryptStartMillis;

			// non encrypted
			props = new Properties();
			props.put("fileExtension", ".csv");
			conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);
			stmt = conn.createStatement();
			long noEncryptStartMillis = System.currentTimeMillis();
			rset = stmt.executeQuery("SELECT * FROM speedtest_decypher");
			while (rset.next())
				noEncryptCount++;
			conn.close();
			long noEncryptEndMillis = System.currentTimeMillis();
			
			timeNoEncrypt += noEncryptEndMillis - noEncryptStartMillis;
		}

		// comparing results
		assertEquals(noEncryptCount, encryptCount);

		assertTrue("timeNoEncrypt = " + timeNoEncrypt
				+ "ms; timeEncrypt = " + timeEncrypt + "ms",
				timeEncrypt <= 30 * timeNoEncrypt);
	}

	@Test
	public void testOpenManyCryptoFiles() throws SQLException, IOException
	{
		// the properties for the connection:
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("cryptoFilterClassName", "org.relique.io.XORCipher");
		props.put("cryptoFilterParameterTypes", "String");
		props.put("cryptoFilterParameters", "gaius vipsanius agrippa");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();

		for (int i = 0; i < testSize; i++)
		{
			ResultSet results = stmt.executeQuery("SELECT * FROM scrambled_" + i);
			assertTrue(results.next());
			while (results.next())
			{
				// nothing really
			}
			results.close();
		}

		stmt.close();
		conn.close();
	}
}
