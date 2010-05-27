package org.relique.jdbc.csv;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Vector;

public class CsvReader {

	private String headerline;
	private CsvRawReader rawReader;
	private int transposedLines;
	private int transposedFieldsToSkip;
	private String[] columnNames;
	public String[] fieldValues;
	private Vector firstTable;
	private int joiningValueNo;
	private int valuesToJoin;
	private String[] joiningValues;

	public CsvReader(CsvRawReader rawReader, int transposedLines,
			int transposedFieldsToSkip, String headerline) throws SQLException {
		this.rawReader = rawReader;
		this.transposedLines = transposedLines;
		this.transposedFieldsToSkip = transposedFieldsToSkip;
		this.headerline = headerline;
		this.columnNames = rawReader.parseCsvLine(headerline, true);
		this.firstTable = null;
		
		if(!this.isPlainReader()) {
			firstTable = new Vector();
			joiningValueNo = 0;
			joiningValues = null;
			try {
				String[] values = null;
				for (int i = 0; i < transposedLines; i++) {
					String line;
					line = rawReader.getNextDataLine();
					values = rawReader.parseCsvLine(line, false);
					firstTable.add(values);
				}
				valuesToJoin = values.length;
				fieldValues = new String[columnNames.length];
			} catch (IOException e) {
				e.printStackTrace();
				throw new SQLException("" + e);
			}
		}
	}

	public int getTransposedFieldsToSkip() {
		return transposedFieldsToSkip;
	}

	public int getTransposedLines() {
		return transposedLines;
	}

	public CsvRawReader getRawReader() {
		return rawReader;
	}

	public String getHeaderline() {
		return headerline;
	}
	
	public boolean next() throws SQLException {
		if(this.isPlainReader()) { 
			boolean result = rawReader.next();
			fieldValues = rawReader.fieldValues;
			return result;
		} else {
			if(joiningValues == null || joiningValueNo > valuesToJoin) {
				String line;
				try {
					line = rawReader.getNextDataLine();
				} catch (IOException e) {
					throw new SQLException("" + e);
				}
				joiningValues = rawReader.parseCsvLine(line, false);
				joiningValueNo = 0;
			}
			for(int i=0; i<transposedLines; i++) {
				fieldValues[i] = ((String[])firstTable.get(i))[joiningValueNo+this.getTransposedFieldsToSkip()];
			}
			for(int i=transposedLines; i<columnNames.length-1; i++) {
				fieldValues[i] = joiningValues[i-transposedLines]; 
			}
			fieldValues[columnNames.length - 1] = joiningValues[columnNames.length - transposedLines - 1 + joiningValueNo];
			joiningValueNo++;
			return true;
		}
	}

	public String[] getColumnNames() {
		if(isPlainReader())
			return rawReader.getColumnNames();
		else
			return columnNames;
	}

	private boolean isPlainReader() {
		return transposedLines == 0 && transposedFieldsToSkip == 0;
	}

	public String getField(int i) throws SQLException {
		if(isPlainReader())
			return rawReader.getField(i);
		else
			return null;
	}

	public void close() {
		rawReader.close();
	}

}
