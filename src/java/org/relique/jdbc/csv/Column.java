package org.relique.jdbc.csv;

public class Column {
	private String name;
	private String value;
	private int position;

	public Column(String value) {
		this.setName(value);
		this.setPosition(-1);
		this.setValue(value);
	}

	public Column(String name, int i, String value) {
		this.setName(name);
		this.setPosition(i);
		this.setValue(value);
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * 
	 * @return the name of the column in the resulting table.
	 */
	public String getName() {
		return name;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setPosition(int i) {
		this.position = i;
	}

	public int getPosition() {
		return position;
	}

	/**
	 * 
	 * @return the name of the column in the database table.<p/>  
	 * 
	 * <b>null</b> for literal values. 
	 */
	public String getDBName() {
		if (position == -1) return null;
		return value;
	}

}
