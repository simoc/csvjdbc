package org.relique.jdbc.csv;

import java.sql.SQLException;

public class ScrollableCsvReader extends CsvReader {

	public ScrollableCsvReader(CsvRawReader rawReader, int transposedLines,
			int transposedFieldsToSkip, String headerline) throws SQLException {
		super(rawReader, transposedLines, transposedFieldsToSkip, headerline);
		// TODO Auto-generated constructor stub
	}

}
