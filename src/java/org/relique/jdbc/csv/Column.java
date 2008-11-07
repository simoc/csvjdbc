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
 * @version $Id: Column.java,v 1.2 2008/11/07 15:36:42 mfrasca Exp $
 * 
 */
package org.relique.jdbc.csv;

public class Column {
	private String name;
	private String value;
	private int position;
	private Class type;

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

	public Column(String name, int i, String value, String typeName) {
		this.setName(name);
		this.setPosition(i);
		this.setValue(value);
		try {
			this.setType(Class.forName(typeName));
		} catch (ClassNotFoundException e) {
			this.setType(null);
		}
	}

	public void setType(Class type) {
		this.type = type;
	}

	public Class getType() {
		return(this.type);
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
	 *         <b>null</b> for literal values.
	 */
	public String getDBName() {
		if (position == -1)
			return null;
		return value;
	}

}
