package org.relique.io;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class EncryptedFileOutputStream extends OutputStream {
	
	private OutputStream out;
	private CryptoFilter filter;

	public EncryptedFileOutputStream(String fileName, CryptoFilter filter) throws FileNotFoundException
	{
		this.filter = filter;
		out = new FileOutputStream(fileName);
	}

	public void write(int b) throws IOException {
		if (filter == null)
			out.write(b);
		else
			filter.write(out, b);
	}

	public void write(String string) throws IOException {
		char[] chars = string.toCharArray();
		for(int pos=0; pos<string.length(); pos++)
			write(chars[pos]);
	}

}
