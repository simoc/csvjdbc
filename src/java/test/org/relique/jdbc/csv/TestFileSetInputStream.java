package test.org.relique.jdbc.csv;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.relique.io.FileSetInputStream;

public class TestFileSetInputStream extends TestCase {

	private String filePath;
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";

	protected void setUp() {
		filePath = System.getProperty(SAMPLE_FILES_LOCATION_PROPERTY);
		if (filePath == null)
			filePath = RunTests.DEFAULT_FILEPATH;
		assertNotNull("Sample files location property not set !", filePath);
		filePath = filePath + "/";

		// load CSV driver
		try {
			Class.forName("org.relique.jdbc.csv.CsvDriver");
		} catch (ClassNotFoundException e) {
			fail("Driver is not in the CLASSPATH -> " + e);
		}

	}

	public void testGlueAsTrailing() throws IOException {
		BufferedReader inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "test-glued-trailing.txt")));

		BufferedReader inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"test-([0-9]{3})-([0-9]{8}).txt", new String[] {
								"location", "file_date" }, ',', false, false, null, 0)));

		Set<String> refSet = new HashSet<String>();
		Set<String> testSet = new HashSet<String>();
		inputRef.readLine();
		inputTest.readLine();
		String lineRef, lineTest;
		do {
			lineRef = inputRef.readLine();
			lineTest = inputTest.readLine();
			refSet.add(lineRef);
			testSet.add(lineTest);
		} while (lineRef != null && lineTest != null);
		assertTrue("refSet contains testSet", refSet.containsAll(testSet));
		assertTrue("testSet contains refSet", testSet.containsAll(refSet));
	}

	public void testGlueAsLeading() throws IOException {
		BufferedReader inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "test-glued-leading.txt")));

		BufferedReader inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"test-([0-9]{3})-([0-9]{8}).txt", new String[] {
								"location", "file_date" }, ',', true, false, null, 0)));

		Set<String> refSet = new HashSet<String>();
		Set<String> testSet = new HashSet<String>();
		inputRef.readLine();
		inputTest.readLine();
		String lineRef, lineTest;
		do {
			lineRef = inputRef.readLine();
			lineTest = inputTest.readLine();
			refSet.add(lineRef);
			testSet.add(lineTest);
		} while (lineRef != null && lineTest != null);
		assertTrue("refSet contains testSet", refSet.containsAll(testSet));
		assertTrue("testSet contains refSet", testSet.containsAll(refSet));
	}

	public void testGlueAsLeadingHeaderless() throws IOException {
		BufferedReader inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "headerless-glued-leading.txt")));

		BufferedReader inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"headerless-([0-9]{3})-([0-9]{8}).txt", new String[] {
								"location", "file_date" }, ',', true, true, null, 0)));

		Set<String> refSet = new HashSet<String>();
		Set<String> testSet = new HashSet<String>();
		String lineRef, lineTest;
		do {
			lineRef = inputRef.readLine();
			lineTest = inputTest.readLine();
			refSet.add(lineRef);
			testSet.add(lineTest);
		} while (lineRef != null && lineTest != null);
		assertTrue("refSet contains testSet", refSet.containsAll(testSet));
		assertTrue("testSet contains refSet", testSet.containsAll(refSet));
	}

	public void testGlueAsEmpty() throws IOException {
		BufferedReader inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "empty-glued.txt")));

		BufferedReader inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"empty-([0-9]+).txt", new String[] {
							"EMPTY_ID"}, ',', false, false, null, 0)));

		Set<String> refSet = new HashSet<String>();
		Set<String> testSet = new HashSet<String>();
		String lineRef, lineTest;
		do {
			lineRef = inputRef.readLine();
			lineTest = inputTest.readLine();
			refSet.add(lineRef);
			testSet.add(lineTest);
		} while (lineRef != null && lineTest != null);
		assertTrue("refSet contains testSet", refSet.containsAll(testSet));
		assertTrue("testSet contains refSet", testSet.containsAll(refSet));
	}
}
