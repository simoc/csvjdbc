/*
 *	CsvJdbc - a JDBC driver for CSV files
 *	Copyright (C) 2014	Simon Chenery
 *
 *	This library is free software; you can redistribute it and/or
 *	modify it under the terms of the GNU Lesser General Public
 *	License as published by the Free Software Foundation; either
 *	version 2.1 of the License, or (at your option) any later version.
 *
 *	This library is distributed in the hope that it will be useful,
 *	but WITHOUT ANY WARRANTY; without even the implied warranty of
 *	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *	Lesser General Public License for more details.
 *
 *	You should have received a copy of the GNU Lesser General Public
 *	License along with this library; if not, write to the Free Software
 *	Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class CsvResources
{
	private static ResourceBundle messages = PropertyResourceBundle.getBundle("org.relique.jdbc.csv.messages", Locale.getDefault());

	public static String getString(String key)
	{
		try
		{
			return messages.getString(key);
		}
		catch (MissingResourceException e)
		{
			return "[" + key + "]";
		}
	}

	public static int getMajorVersion()
	{
		return parseVersion(getVersionString(), 0,1);
	}

	public static int getMinorVersion()
	{
		return parseVersion(getVersionString(), 1,0);
	}

	public static String getVersionString()
	{
		return messages.containsKey("versionString") ? CsvResources.getString("versionString"): "1.0";
	}

	public  static int parseVersion(String versionString, int index, int defaultValue)
	{
		try {
			if (versionString != null) {
				return Integer.parseInt(versionString.split("\\.")[index]);
			}
		} catch (NumberFormatException e)
		{
			// We just return the default
		}
		return defaultValue;
	}

}
