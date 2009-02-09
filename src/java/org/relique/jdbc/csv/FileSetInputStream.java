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

package org.relique.jdbc.csv;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that collapses a set of files into one input stream. All files matching
 * a given pattern are collected, parts of the file name contains part of the
 * data, and the values in the file name are appended (or prepended) to each
 * data line.
 * 
 * @author Mario Frasca
 * 
 */
public class FileSetInputStream extends InputStream {

	private List fileNames;
	private FileInputStream currentFile;
	private boolean atLineBorder;
	private boolean readingHeader;
	private String tail;
	private int pos;
	private Pattern fileNameRE;
	private char separator;
	private String dataTail;
	private boolean prepend;
	private boolean atBeginningOfLine;

	/**
	 * 
	 * @param dirName
	 *            the containing directory
	 * @param fileNamePattern
	 *            the regular expression describing the file name and the extra
	 *            fields.
	 * @param fieldsInName
	 *            the names of the fields contained in the file name.
	 * @param separator
	 *            the separator to use when faking output (typically the ",").
	 * @param prepend
	 *            whether the extra fields should precede the ones from the file
	 *            content.
	 * @throws FileNotFoundException
	 */
	public FileSetInputStream(String dirName, String fileNamePattern,
			String[] fieldsInName, char separator, boolean prepend)
			throws FileNotFoundException {

		// Initialising tail for header...
		this.prepend = prepend;
		this.separator = separator;
		tail = "";
		if (!prepend)
			tail += separator;
		for (int i = 0; i < fieldsInName.length; i++) {
			tail += fieldsInName[i];
			if(i+1 < fieldsInName.length)
				tail += separator;
		}

		fileNames = new LinkedList();
		File root = new File(dirName);
		String[] candidates = root.list();

		fileNameRE = Pattern.compile(fileNamePattern);

		for (int i = 0; i < candidates.length; i++) {
			Matcher m = fileNameRE.matcher(candidates[i]);
			if (m.matches()) {
				fileNames.add(dirName + candidates[i]);
			}
		}
		fileNameRE = Pattern.compile(".*" + fileNamePattern);
		readingHeader = true;
		this.atLineBorder = prepend;
		String currentName = (String) fileNames.remove(0);
		dataTail = getTailFromName(currentName);
		currentFile = new FileInputStream(currentName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.FileInputStream#close()
	 */
	public void close() throws IOException {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.FileInputStream#read()
	 */
	public int read() throws IOException {
		if (atLineBorder) {
			return readTail();
		}
		if (currentFile == null)
			return -1;
		int ch = currentFile.read();
		if (ch == '\n') {
			atLineBorder = true;
			if (!prepend)
				return readTail();
			else
				return ch;
		} else if (ch == -1) {
			currentFile.close();
			// open next file and skip header
			atLineBorder = false;
			pos = 0;
			String currentName;
			try {
				currentName = (String) fileNames.remove(0);
			} catch (IndexOutOfBoundsException e) {
				currentFile = null;
				return -1;
			}
			tail = getTailFromName(currentName);
			currentFile = new FileInputStream(currentName);
			while (currentFile.read() != '\n')
				;
			return read();
		}
		return ch;
	}

	private String getTailFromName(String currentName) {
		Matcher m = fileNameRE.matcher(currentName);
		m.matches();
		String tail = "";
		if (!prepend)
			tail += separator;
		for (int i = 1; i <= m.groupCount(); i++) {
			tail += m.group(i);
			if (i<m.groupCount())
				tail += separator;
		}
		return tail;
	}

	private int readTail() {
		if (pos < tail.length())
			return tail.charAt(pos++);
		atLineBorder = false;
		pos = 0;
		if (readingHeader) {
			readingHeader = false;
			tail = dataTail;
		}
		if (prepend)
			return separator;
		else
			return '\n';
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.InputStream#reset()
	 */
	public synchronized void reset() throws IOException {
		super.reset();
	}

}
