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
		this.filter.reset();
		out = new FileOutputStream(fileName);
	}

	public void write(int b) throws IOException {
		if (filter == null)
			out.write(b);
		else
			filter.write(out, b);
	}

}
