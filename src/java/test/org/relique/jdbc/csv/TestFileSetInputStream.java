package test.org.relique.jdbc.csv;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.relique.jdbc.csv.FileSetInputStream;

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
		String lineRef, lineTest;
		BufferedReader inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "test-glued-trailing.txt")));

		BufferedReader inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"test-([0-9]{3})-([0-9]{8}).txt", new String[] {
								"location", "file_date" }, ',', false)));
		int i = 0;
		do {
			i++;
			lineRef = inputRef.readLine();
			lineTest = inputTest.readLine();
			assertEquals("on line " + i, lineRef, lineTest);
		} while (lineRef != null);
	}

	public void testGlueAsLeading() throws IOException {
		String lineRef, lineTest;
		BufferedReader inputRef = new BufferedReader(new InputStreamReader(
				new FileInputStream(filePath + "test-glued-leading.txt")));

		BufferedReader inputTest = new BufferedReader(new InputStreamReader(
				new FileSetInputStream(filePath,
						"test-([0-9]{3})-([0-9]{8}).txt", new String[] {
								"location", "file_date" }, ',', true)));

		int i = 0;
		do {
			i++;
			lineRef = inputRef.readLine();
			lineTest = inputTest.readLine();
			assertEquals("on line " + i, lineRef, lineTest);
		} while (lineRef != null);
	}

}
