package org.relique.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class XORCipher implements CryptoFilter {
	
	private int keyCounter;
	private int[] scrambleKey;

	public XORCipher(String seed){
		scrambleKey = new int[seed.length()];
		for(int i=0; i<seed.length(); i++)
			scrambleKey[i] = seed.charAt(i);
		keyCounter = 0;
	}

	public int read(InputStream in) throws IOException {
		if (in.available() > 0) {
			return (byte) scrambleInt(in.read());
		} else
			return -1;
	}
	
	public int read(InputStream in,byte[] b) throws IOException {
		int len;
		len = in.read(b, 0, b.length);
		scrambleArray(b);
		return len;
	}

	public int read(InputStream in,byte[] b, int off, int len) throws IOException {
		len = in.read(b, off, len);
		scrambleArray(b);
		return len;
	}

	public String toString(){
		return "XORCipher("+scrambleKey.length+"):'"+scrambleKey+"'";
	}

	/**
	 * Perform the scrambling algorithm (named bitwise XOR encryption) using
	 * bitwise exclusive OR (byte) encrypted character = (byte) original
	 * character ^ (byte) key character.
	 * <p/>
	 * Note that ^ is the symbol for XOR (and not the mathematical power)
	 * 
	 * @param org
	 *            is the original value
	 * @return
	 */
	private int scrambleInt(int org) {
		int encrDataChar = org ^ scrambleKey[keyCounter];
		keyCounter++;
		keyCounter %= scrambleKey.length;
		return encrDataChar;
	}

	/**
	 * Perform the scrambling algorithm (named bitwise XOR encryption) using
	 * bitwise exclusive OR (byte) encrypted character = (byte) original
	 * character ^ (byte) key character.
	 * <p/>
	 * Note that ^ is the symbol for XOR (and not the mathematical power)
	 * 
	 * @param org
	 *            is the original byte array
	 * @return
	 */
	private void scrambleArray(byte[] org) {
		for (int i = 0; i < org.length; i++) {
			org[i] ^= scrambleKey[keyCounter];
			keyCounter++;
			keyCounter %= scrambleKey.length;
		}
	}
	
	public void write(OutputStream out, int ch) throws IOException {
		out.write(scrambleInt(ch));
	}

	public void reset() {
		keyCounter = 0;		
	}

}
