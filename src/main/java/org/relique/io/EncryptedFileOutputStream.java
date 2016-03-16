/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.io;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class EncryptedFileOutputStream extends OutputStream
{
	private OutputStream out;
	private CryptoFilter filter;

	public EncryptedFileOutputStream(String fileName, CryptoFilter filter)
			throws FileNotFoundException
	{
		this.filter = filter;
		this.filter.reset();
		out = new FileOutputStream(fileName);
	}

	public void write(int b) throws IOException
	{
		if (filter == null)
			out.write(b);
		else
			filter.write(out, b);
	}

	public void close() throws IOException
	{
		if (out != null)
			out.close();
	}
}
