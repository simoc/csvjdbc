package org.relique.jdbc.csv;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringConverter {

	private String dateFormat;
	private String timeFormat;

	public StringConverter(String dateformat, String timeformat){
		dateFormat = dateformat;
		timeFormat = timeformat;
	}
	
	public String parseString(String str) {
		return str;
	}

	public boolean parseBoolean(String str) {
		return Boolean.valueOf(str).booleanValue();
	}

	public byte parseByte(String str) {
		try {
			return (str == null) ? 0 : Byte.parseByte(str);
		} catch (RuntimeException e) {
			return 0;
		}
	}

	public short parseShort(String str) {
		try {
			return (str == null) ? 0 : Short.parseShort(str);
		} catch (RuntimeException e) {
			return 0;
		}
	}

	public int parseInt(String str) {
		try {
			return (str == null) ? 0 : Integer.parseInt(str);
		} catch (RuntimeException e) {
			return 0;
		}
	}

	public long parseLong(String str) {
		try {
			return (str == null) ? 0 : Long.parseLong(str);
		} catch (RuntimeException e) {
			return 0;
		}
	}

	public float parseFloat(String str) {
		try {
			return (str == null) ? 0 : Float.parseFloat(str);
		} catch (RuntimeException e) {
			return 0;
		}
	}

	public double parseDouble(String str) {
		try {
			return (str == null) ? 0 : Double.parseDouble(str);
		} catch (RuntimeException e) {
			return 0;
		}
	}

	public byte[] parseBytes(String str) {
		try {
			return (str == null) ? null : str.getBytes();
		} catch (RuntimeException e) {
			return null;
		}
	}

	public BigDecimal parseBigDecimal(String str) {
		try {
			return (str == null) ? null : new BigDecimal(str);
		} catch (RuntimeException e) {
			return null;
		}
	}

	/**
	 * transforms the date string into its equivalent ISO8601
	 * 
	 * @param date
	 * @param format
	 * @return
	 */
	private String makeISODate(String date, String format) {
		// first memorize the original order of the groups.
		format = format.toLowerCase();
		int dpos = format.indexOf('d');
		int mpos = format.indexOf('m');
		int ypos = format.indexOf('y');

		int day = 1, month = 1, year = 1;
		if(dpos > mpos) day+=1; else month+=1;
		if(dpos > ypos) day+=1; else year+=1;
		if(mpos > ypos) month+=1; else year+=1;

		// then build the regular expression
		Pattern part;
		Matcher m;
		
		part = Pattern.compile("d+");
		m = part.matcher(format);
		if (m.find())
			format = format.replace(m.group(), "([0-9]{" + (m.end()-m.start()) + ",2})");
		
		part = Pattern.compile("m+");
		m = part.matcher(format);
		if (m.find())
			format = format.replace(m.group(), "([0-9]{" + (m.end()-m.start()) + ",2})");

		part = Pattern.compile("y+");
		m = part.matcher(format);
		if (m.find())
			format = format.replace(m.group(), "([0-9]{" + (m.end()-m.start()) + ",4})");
		
		format = format + ".*";
		
		Pattern pattern = Pattern.compile(format);
		m = pattern.matcher(date);
		if(m.matches())
			// and return the groups in the ISO8601 order.
			return m.group(year) + "-" + m.group(month) + "-" + m.group(day);
		else
			return "1970-01-01";
	}
	
	public Date parseDate(String str) {
		try {
			Date sqlResult = Date.valueOf(makeISODate(str, dateFormat));
			return sqlResult;
		} catch (RuntimeException e) {
			return null;
		}
	}

	public Time parseTime(String str) {
		try {
			str = str.trim();
			while (str.length() < timeFormat.length()){
				str = "0" + str;
			}
			String hours = "00";
			int pos = timeFormat.indexOf('H'); 
			if (pos != -1){
				hours = str.substring(pos, pos+2);
			}
			String minutes = "00";
			pos = timeFormat.indexOf('m'); 
			if (pos != -1){
				minutes = str.substring(pos, pos+2);
			}
			String seconds = "00";
			pos = timeFormat.indexOf('s'); 
			if (pos != -1){
				seconds = str.substring(pos, pos+2);
			}
			Time sqlResult = Time.valueOf(hours+":"+minutes+":"+seconds);
			return sqlResult;
		} catch (RuntimeException e) {
			return null;
		}
	}

	public Timestamp parseTimestamp(String str) {
		try {
			return (str == null) ? null : Timestamp.valueOf(str);
		} catch (RuntimeException e) {
			return null;
		}
	}

	public InputStream parseAsciiStream(String str) {
		return (str == null) ? null : new ByteArrayInputStream(str.getBytes());
	}
	
	static protected Map forSQLNameMap = new HashMap(){
		private static final long serialVersionUID = -3037117163532338893L;
		{
			try{
				put("String", Class.forName("java.lang.String"));
				put("Boolean", Class.forName("java.lang.Boolean"));
				put("Byte", Class.forName("java.lang.Byte"));
				put("Short", Class.forName("java.lang.Short"));
				put("Int", Class.forName("java.lang.Integer"));
				put("Long", Class.forName("java.lang.Long"));
				put("Float", Class.forName("java.lang.Float"));
				put("Double", Class.forName("java.lang.Double"));
				put("BigDecimal", Class.forName("java.math.BigDecimal"));
				put("Date", Class.forName("java.sql.Date"));
				put("Time", Class.forName("java.sql.Time"));
				put("Timestamp", Class.forName("java.sql.Timestamp"));
				put("AsciiStream", Class.forName("java.io.InputStream"));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	};
	
	static protected Map converterMethodForClass = new HashMap(){
		private static final long serialVersionUID = -3037117163532338893L;
		Class[] argTypes = new Class[1];
		Class containerClass = null;
		{
			try {
				argTypes[0] = Class.forName("java.lang.String");
				containerClass = Class.forName("org.relique.jdbc.csv.StringConverter");
				put("String", containerClass.getMethod("parseString", argTypes));
				put("Boolean", containerClass.getMethod("parseBoolean", argTypes));
				put("Byte", containerClass.getMethod("parseByte", argTypes));
				put("Short", containerClass.getMethod("parseShort", argTypes));
				put("Int", containerClass.getMethod("parseInt", argTypes));
				put("Long", containerClass.getMethod("parseLong", argTypes));
				put("Float", containerClass.getMethod("parseFloat", argTypes));
				put("Double", containerClass.getMethod("parseDouble", argTypes));
				put("BigDecimal", containerClass.getMethod("parseBigDecimal", argTypes));
				put("Date", containerClass.getMethod("parseDate", argTypes));
				put("Time", containerClass.getMethod("parseTime", argTypes));
				put("Timestamp", containerClass.getMethod("parseTimestamp", argTypes));
				put("AsciiStream", containerClass.getMethod("parseAsciiStream", argTypes));
				/*
				 * sooner or later, maybe...
				 * put("UnicodeStream", containerClass.getMethod("parseUnicodeStream", argTypes));
				 * put("BinaryStream", containerClass.getMethod("parseBinaryStream", argTypes));
				 * put("Blob", containerClass.getMethod("parseBlob", argTypes));
				 * put("Clob", containerClass.getMethod("parseClob", argTypes));
				 * put("Array", containerClass.getMethod("parseArray", argTypes));
				 * put("URL", containerClass.getMethod("parseURL", argTypes));
				 * put("NCharacterStream", containerClass.getMethod("parseNCharacterStream", argTypes));
				 * put("NClob", containerClass.getMethod("parseNClob", argTypes));
				 * put("NString", containerClass.getMethod("parseNString", argTypes));
				 * put("RowId", containerClass.getMethod("parseRowId", argTypes));
				 * put("SQLXML", containerClass.getMethod("parseSQLXML", argTypes));
				 */
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			}
		}
	};

	public Object convert(String sqlTypeName, String stringRepresentation) {
    	
		Object value = stringRepresentation;
    	if (sqlTypeName != null) {
    		Object[] args = new Object[1];
    		args[0] = stringRepresentation;
			try {
				value = ((Method)(converterMethodForClass.get(sqlTypeName))).invoke(this, args);
			} catch (IllegalArgumentException e) {
			} catch (IllegalAccessException e) {
			} catch (InvocationTargetException e) {
			}
    	}
		return value;
	}

	public Class forSQLName(String string) {
		return (Class)forSQLNameMap.get(string);
	}
}
