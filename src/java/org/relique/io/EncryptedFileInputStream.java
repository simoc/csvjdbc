package org.relique.io;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class EncryptedFileInputStream extends InputStream {
	
	private InputStream in;
	private CryptoFilter filter;

	public EncryptedFileInputStream(String fileName, CryptoFilter filter)
			throws FileNotFoundException {
		this.filter = filter;
		in = new BufferedInputStream(new FileInputStream(fileName));
	}

	public int read() throws IOException {
		if (filter != null)
			return filter.read(in);
		else
			return in.read();
	}
	
	public int read(byte[] b, int off, int len) throws IOException {
		if (filter != null)
			return filter.read(in, b, off, len);
		else
			return in.read(b, off, len);
	}

	public int read(InputStream in, byte[] b) throws IOException {
		if (filter != null)
			return filter.read(in, b);
		else
			return in.read(b);
	}
	
	public void close() throws IOException {
		if (in == null) {
			return;
		}
		in.close();
	}

}
