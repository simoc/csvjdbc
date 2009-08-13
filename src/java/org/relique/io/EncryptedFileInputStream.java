package org.relique.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class EncryptedFileInputStream extends InputStream {
	
	private InputStream in;
	private CryptoFilter filter;

	public EncryptedFileInputStream(String fileName, CryptoFilter filter) throws FileNotFoundException
	{
		this.filter = filter;
		in = new FileInputStream(fileName);
	}

	public int read() throws IOException {
		if (filter != null)
			return filter.read(in);
		else
			return in.read();
	}

}
