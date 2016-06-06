/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2008  Mario Frasca
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.relique.io.FileSetInputStream;

public class TestFileSetInputStream
{
	private static String filePath;

	@BeforeClass
	public static void setUp()
	{
		filePath = ".." + File.separator + "src" + File.separator + "testdata";
		if (!new File(filePath).isDirectory())
			filePath = "src" + File.separator + "testdata";
		assertTrue("Sample files directory not found: " + filePath, new File(filePath).isDirectory());
		filePath = filePath + File.separator;

		// load CSV driver
		try
		{
			Class.forName("org.relique.jdbc.csv.CsvDriver");
		}
		catch (ClassNotFoundException e)
		{
			fail("Driver is not in the CLASSPATH -> " + e);
		}
	}

	@Test
	public void testGlueAsTrailing() throws IOException
	{
		BufferedReader inputRef = null;
		BufferedReader inputTest = null;

		try
		{
			inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "test-glued-trailing.txt")));

			inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"test-([0-9]{3})-([0-9]{8}).txt", new String[] {
								"location", "file_date" }, ",", false, false, null, 0)));

			Set<String> refSet = new HashSet<String>();
			Set<String> testSet = new HashSet<String>();
			inputRef.readLine();
			inputTest.readLine();
			String lineRef, lineTest;
			do
			{
				lineRef = inputRef.readLine();
				lineTest = inputTest.readLine();
				refSet.add(lineRef);
				testSet.add(lineTest);
			}
			while (lineRef != null && lineTest != null);
			assertTrue("refSet contains testSet", refSet.containsAll(testSet));
			assertTrue("testSet contains refSet", testSet.containsAll(refSet));
		}
		finally
		{
			if (inputRef != null)
				inputRef.close();
			if (inputTest != null)
				inputTest.close();
		}
	}

	@Test
	public void testGlueAsLeading() throws IOException
	{
		BufferedReader inputRef = null;
		BufferedReader inputTest = null;

		try
		{
			inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "test-glued-leading.txt")));

			inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"test-([0-9]{3})-([0-9]{8}).txt", new String[] {
								"location", "file_date" }, ",", true, false, null, 0)));

			Set<String> refSet = new HashSet<String>();
			Set<String> testSet = new HashSet<String>();
			inputRef.readLine();
			inputTest.readLine();
			String lineRef, lineTest;
			do
			{
				lineRef = inputRef.readLine();
				lineTest = inputTest.readLine();
				refSet.add(lineRef);
				testSet.add(lineTest);
			}
			while (lineRef != null && lineTest != null);
			assertTrue("refSet contains testSet", refSet.containsAll(testSet));
			assertTrue("testSet contains refSet", testSet.containsAll(refSet));
		}
		finally
		{
			if (inputRef != null)
				inputRef.close();
			if (inputTest != null)
				inputTest.close();
		}
	}

	@Test
	public void testGlueAsLeadingHeaderless() throws IOException
	{
		BufferedReader inputRef = null;
		BufferedReader inputTest = null;

		try
		{
			inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "headerless-glued-leading.txt")));

			inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"headerless-([0-9]{3})-([0-9]{8}).txt", new String[] {
								"location", "file_date" }, ",", true, true, null, 0)));

			Set<String> refSet = new HashSet<String>();
			Set<String> testSet = new HashSet<String>();
			String lineRef, lineTest;
			do
			{
				lineRef = inputRef.readLine();
				lineTest = inputTest.readLine();
				refSet.add(lineRef);
				testSet.add(lineTest);
			}
			while (lineRef != null && lineTest != null);
			assertTrue("refSet contains testSet", refSet.containsAll(testSet));
			assertTrue("testSet contains refSet", testSet.containsAll(refSet));
		}
		finally
		{
			if (inputRef != null)
				inputRef.close();
			if (inputTest != null)
				inputTest.close();
		}
	}

	@Test
	public void testGlueAsEmpty() throws IOException
	{
		BufferedReader inputRef = null;
		BufferedReader inputTest = null;

		try
		{
			inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "empty-glued.txt")));

			inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"empty-([0-9]+).txt", new String[] {
							"EMPTY_ID"}, ",", false, false, null, 0)));

			Set<String> refSet = new HashSet<String>();
			Set<String> testSet = new HashSet<String>();
			String lineRef, lineTest;
			do
			{
				lineRef = inputRef.readLine();
				lineTest = inputTest.readLine();
				refSet.add(lineRef);
				testSet.add(lineTest);
			}
			while (lineRef != null && lineTest != null);
			assertTrue("refSet contains testSet", refSet.containsAll(testSet));
			assertTrue("testSet contains refSet", testSet.containsAll(refSet));
		}
		finally
		{
			if (inputRef != null)
				inputRef.close();
			if (inputTest != null)
				inputTest.close();
		}
	}
	
	@Test
	public void testFileSetInputStreamClose() throws IOException
	{
		FileSetInputStream in = new FileSetInputStream(filePath,
					"test-([0-9]{3})-([0-9]{8}).txt", new String[] {
					"location", "file_date"}, ",", false, false, null, 0);

		in.read();
		in.read();
		in.close();
		try
		{
			in.read();
			fail("expected exception java.io.IOException");
		}
		catch (IOException e)
		{
			assertTrue(("" + e).contains("IOException"));
		}
	}
}
