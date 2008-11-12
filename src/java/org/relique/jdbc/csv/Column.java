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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/** 
 * utility class used to hold information about a column in a SQL query...  
 * it helps keep track of the original position of the column in the table, 
 * if the the column is really coming from the database table,
 * the type of the data for the column for proper type conversion.
 * 
 * @author Mario Frasca
 * @version $Id: Column.java,v 1.4 2008/11/12 16:06:56 mfrasca Exp $
 * 
 */
package org.relique.jdbc.csv;

public class Column {
	private String name;
	private String value;
	private int position;
	private String typeName;

	public Column(String value) {
		setName(value);
		setPosition(-1);
		setValue(value);
		setTypeName(null);
	}

	public Column(String name, int i, String value) {
		setName(name);
		setPosition(i);
		setValue(value);
		setTypeName(null);
	}

	public Column(String name, int i, String value, String typeName) {
		setName(name);
		setPosition(i);
		setValue(value);
		setTypeName(typeName);
	}
	
	public void setTypeName(String type) {
		this.typeName = type;
	}

	public String getTypeName() {
		return(this.typeName);
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
		if (position == -1)
			return value;
		return null;
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
	 *         <b>null</b> for literal values.
	 */
	public String getDBName() {
		if (position == -1)
			return null;
		return value;
	}

}
