package org.relique.jdbc.csv;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.relique.io.DataReader;

public class CsvReader extends DataReader {

	private String headerline;
	CsvRawReader rawReader;
	int transposedLines;
	private int transposedFieldsToSkip;
	String[] columnNames;
	private String[] columnTypes;
	Vector<String []> firstTable;
	int joiningValueNo;
	int valuesToJoin;
	String[] joiningValues;
	private StringConverter converter;
	private String[] fieldValues;

	public CsvReader(CsvRawReader rawReader, int transposedLines,
			int transposedFieldsToSkip, String headerline) throws SQLException {
		super();
		
		this.rawReader = rawReader;
		this.transposedLines = transposedLines;
		this.transposedFieldsToSkip = transposedFieldsToSkip;
		this.headerline = headerline;
		this.columnNames = rawReader.parseLine(headerline, true);
		this.firstTable = null;
		columnTypes = null;
		
		if(!this.isPlainReader()) {
			firstTable = new Vector<String []>();
			joiningValueNo = 0;
			joiningValues = null;
			try {
				String[] values = null;
				for (int i = 0; i < transposedLines; i++) {
					String line;
					line = rawReader.getNextDataLine();
					values = rawReader.parseLine(line, false);
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

	public void setConverter(StringConverter converter) {
		this.converter = converter;
	}

	public int getTransposedFieldsToSkip() {
		return transposedFieldsToSkip;
	}

	public int getTransposedLines() {
		return transposedLines;
	}

	public String getHeaderline() {
		return headerline;
	}
	
	boolean isPlainReader() {
		return transposedLines == 0 && transposedFieldsToSkip == 0;
	}

	public boolean next() throws SQLException {
		if(this.isPlainReader()) { 
			boolean result = rawReader.next();
			fieldValues = rawReader.fieldValues;
			return result;
		} else {
			if(joiningValues == null || joiningValueNo + getTransposedFieldsToSkip() == valuesToJoin) {
				String line;
				try {
					line = rawReader.getNextDataLine();
				} catch (IOException e) {
					throw new SQLException("" + e);
				}
				if(line == null)
					return false;
				joiningValues = rawReader.parseLine(line, false);
				joiningValueNo = 0;
			}
			for(int i=0; i<transposedLines; i++) {
				fieldValues[i] = firstTable.get(i)[joiningValueNo + getTransposedFieldsToSkip()];
			}
			for(int i=transposedLines; i<columnNames.length-1; i++) {
				fieldValues[i] = joiningValues[i-transposedLines]; 
			}
			fieldValues[columnNames.length - 1] = joiningValues[columnNames.length - transposedLines - 1 + joiningValueNo];
			joiningValueNo++;
			if(columnTypes == null)
				getColumnTypes();
			return true;
		}
	}

	public String[] getColumnNames() {
		if(isPlainReader())
			return rawReader.getColumnNames();
		else
			return columnNames;
	}

	public Object getField(int i) throws SQLException {
		if(isPlainReader())
			return rawReader.getField(i);
		else
			return null;
	}

	public void close() {
		rawReader.close();
	}

	public Map<String, Object> getEnvironment() throws SQLException {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("@STRINGCONVERTER", converter);
		if(fieldValues.length != getColumnNames().length)
			throw new SQLException("data contains " + fieldValues.length + " columns, expected " + getColumnNames().length);
		if(columnTypes == null)
			getColumnTypes();
		String tableAlias = rawReader.getTableAlias();
		for (int i = 0; i < getColumnNames().length; i++) {
			String key = getColumnNames()[i].toUpperCase();
			Object value = converter.convert(columnTypes[i], fieldValues[i]);
			result.put(key, value);
			if (tableAlias != null) {
				/*
				 * Also allow column value to be accessed as S.ID  if table alias S is set.
				 */
				result.put(tableAlias + "." + key, value);
			}
			
		}
		return result;
	}
	
	public void setColumnTypes(String line) throws SQLException {
    	String[] typeNamesLoc = line.split(",");
    	if (typeNamesLoc.length == 0)
    		throw new SQLException("Invalid column types: " + line);
    	columnTypes = new String[getColumnNames().length];
    	for(int i=0; i<Math.min(typeNamesLoc.length, columnTypes.length); i++){
    		String typeName = typeNamesLoc[i].trim();
    		if (converter.forSQLName(typeName) == null)
    			throw new SQLException("Invalid column type: " + typeName);
    		columnTypes[i] = typeName;
    	}
    	/*
    	 * Use last column type for any remaining columns.
    	 */
    	for(int i=typeNamesLoc.length; i < columnTypes.length; i++){
    		columnTypes[i] = typeNamesLoc[typeNamesLoc.length-1].trim();
    	}
	}

	public String[] getColumnTypes() {
		if(columnTypes == null) {
			inferColumnTypes();
		}
		return columnTypes;
	}

	private void inferColumnTypes() {
		columnTypes = new String[fieldValues.length];
		for (int i=0; i < fieldValues.length; i++) {
			try {
				String typeName = "String";
				String value = getField(i).toString();
				if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
					typeName = "Boolean";
				} else if (value.equals(("" + converter.parseInt(value)))) {
					typeName = "Int";
				} else if (value.equals(("" + converter.parseLong(value)))) {
					typeName = "Long";
				} else if (value.equals(("" + converter.parseDouble(value)))) {
					typeName = "Double";
				} else if (value.equals(("" + converter.parseBytes(value)))) {
					typeName = "Bytes";
				} else if (value.equals(("" + converter.parseBigDecimal(value)))) {
					typeName = "BigDecimal";
				} else if (converter.parseTimestamp(value) != null) {
					typeName = "Timestamp";
				} else if (value.equals(("" + converter.parseDate(value) + "          ").substring(0, 10))) {
					typeName = "Date";
				} else if (value.equals(("" + converter.parseTime(value) + "        ").substring(0, 8))) {
					typeName = "Time";
				} else if (value.equals(("" + converter.parseAsciiStream(value)))) {
					typeName = "AsciiStream";
				}
				columnTypes[i] = typeName;
			} catch (SQLException e) {
			}
	    }
	}

	public int[] getColumnSizes() {
		return rawReader.getColumnSizes();
	}

	public String getTableAlias() {
		return rawReader.getTableAlias();
	}
}
