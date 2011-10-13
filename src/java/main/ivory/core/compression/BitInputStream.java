
/*
 * Terrier - Terabyte Retriever 
 * Webpage: http://ir.dcs.gla.ac.uk/terrier 
 * Contact: terrier{a.}dcs.gla.ac.uk
 * University of Glasgow - Department of Computing Science
 * http://www.gla.ac.uk/
 * 
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is BitInputStream.java.
 *
 * The Original Code is Copyright (C) 2004-2008 the University of Glasgow.
 * All Rights Reserved.
 *
 * Contributor(s):
 *  Roi Blanco
 */

package ivory.core.compression;


import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
//import uk.ac.gla.terrier.utility.Files;
/**
 * This class reads from a file or an InputStream integers that can be coded with different encoding algorithms.
 * It does not use any internal buffering, and operates with bytes.
 * @author Roi Blanco
 *
 */
public class BitInputStream  {
	
	/** The private input stream used internaly.*/
	protected DataInputStream dis = null;
	/** The byte offset.*/
	protected long byteOffset;
	/** The bit offset.*/	
	protected int bitOffset;
	/** 
	 * A byte read from the stream. This byte should be 
	 * initialised during the construction of the class.
	 */
	protected byte byteRead;	


	/** Do nothing constructor used by child classes which override all methods, eg OldBitInputStream */
	protected BitInputStream(){} 

	/**
	 * Constructs an instance of the class for a given stream
	 * @param is java.io.InputStream the underlying input stream
	 * @throws java.io.IOException if an I/O error occurs
	 */
	public BitInputStream(InputStream is) throws IOException {
		dis = new DataInputStream(is);
		byteOffset = 0;
		bitOffset = 0;
		byteRead = dis.readByte();
	}
	/** 
	 * Constructs an instance of the class for a given filename
	 * @param filename java.lang.String the name of the undelying file
	 * @throws java.io.IOException if an I/O error occurs
	 */
	/*public BitInputStream(String filename) throws IOException {
		dis = new DataInputStream(Files.openFileStream(filename));
		byteOffset = 0;
		bitOffset = 0;
		byteRead = dis.readByte();
	}*/
	/**
	 * Constructs an instance of the class for a given file
	 * @param file java.io.File the underlying file
	 * @throws java.io.IOException if an I/O error occurs
	 */
	/*public BitInputStream(File file) throws IOException {
		dis = new DataInputStream(Files.openFileStream(file));
		byteOffset = 0;
		bitOffset = 0;
		byteRead = dis.readByte();
	}*/
	/** 
	 * Closes the stream.
	 * @throws java.io.IOException if an I/O error occurs
	 */
	public void close() throws IOException {
		dis.close();
	}
	
	/**
	 * Returns the byte offset of the stream. 
	 * It corresponds to the offset of the 
	 * byte from which the next bit will be read.
	 * @return the byte offset in the stream.
	 */
	public long getByteOffset() {
		return byteOffset;
	}
	/**
	 * Returns the bit offset in the last byte. It 
	 * corresponds to the next bit that it will be
	 * read.
	 * @return the bit offset in the stream.
	 */
	
	public byte getBitOffset() {
		return (byte) bitOffset;
	}
	
	/**
	 * Reads a unary encoded integer from the underlying stream 
	 * @return the number read
	 * @throws IOException if an I/O error occurs
	 */
	public int readUnary() throws IOException{
		int x;
		final int leftA = (byteRead << bitOffset) & 0x00FF;		
		if(leftA != 0){
			// the number is in the buffer
			x = 8 - BitUtilities.MSB_BYTES[ leftA ];
			bitOffset += x ;
			readIn();
			return x;
		}
		x = 8 - bitOffset;
		byteOffset++;
		while( ( byteRead = dis.readByte() ) == 0 ) {
			x += 8; 
			byteOffset++;
		}
		x += (bitOffset =  8 -  BitUtilities.MSB_BYTES[ byteRead & 0x00FF] );
		readIn();
		return x;		
	}
	
	/**
	 * Reads a gamma encoded integer from the underlying stream
	 * @return the number read
	 * @throws IOException if an I/O error occurs
	 */
	public int readGamma() throws IOException{
		int u = readUnary() - 1;		
		return (1 << u) + readBinary(u) ;
	}
	
	/**
	 * Reads a binary integer from the already read buffer.
	 * @param len the number of binary bits to read
	 * @throws IOException if an I/O error occurs
	 * @return the decoded integer
	 */
	public int readBinary(int len) throws IOException{
		// the number is in the buffer
		if(8 - bitOffset > len){			   
				int b = ( ( byteRead << bitOffset) & 0x00FF) >>> (8-len);
			   bitOffset += len;
			   return b;
		}
	 
		int x = byteRead & ( ~ (0xFF << (8-bitOffset) )) &0xFF;
		//read the next bytes
		len +=  bitOffset - 8;
		int i = len >> 3;
		while(i-- != 0){
			byteRead = dis.readByte();
			byteOffset++;
			x = x << 8 | (byteRead & 0xFF); //tba pal otro lao
		}
		byteRead = dis.readByte();
		byteOffset++;
		bitOffset = len & 7;	
		return (x << bitOffset)  | ((byteRead & 0xFF) >>> (8-bitOffset)) ;		
	}

	/** Skip a number of bits in the current input stream
	  * @param len The number of bits to skip
	  * @throws IOException if an I/O error occurs
	  */
	public void skipBits(int len) throws IOException
	{
		// the number of bits to skip is within the current byte
		if(8 - bitOffset > len){
			bitOffset += len;
			return;
		}
	
		len +=  bitOffset - 8;
		final int i = len >> 3;
		if (i > 0)
		{
			dis.skipBytes(i);
			byteOffset+= i;
		}
		byteRead = dis.readByte();
		byteOffset++;
		bitOffset = len & 7;
	}
	
	/**
	 * Reads a new byte from the InputStream if we have finished with the current one. 
	 * @throws IOException if we have reached the end of the file
	 */
	private void readIn() throws IOException{
		if(bitOffset == 8){
			byteRead = dis.readByte();
			bitOffset = 0;
			byteOffset++;
		}
	}	
	
	
	/**
	 * Reads a binary encoded integer, given an upper bound
	 * @param b the upper bound
	 * @return the int read
	 * @throws IOException if an I/O error occurs
	 */
	public int readMinimalBinary( final int b ) throws IOException {	
		final int log2b = BitUtilities.mostSignificantBit(b);
		final int m = ( 1 << log2b + 1 ) - b; 
		final int x = readBinary( log2b );		
		if ( x < m ) return x;
		else return ( x << 1 ) + readBinary(1) - m   ;						
	}
	
	/**
	 * Reads a Golomb encoded integer 
	 * @param b the golomb modulus
	 * @return the int read 
	 * @throws IOException if and I/O error occurs
	 */
	public int readGolomb( final int b) throws IOException {		
		final int q = (readUnary() - 1 ) * b;
		return q + readMinimalBinary( b ) + 1;
	}
	
	/**
	 * Reads a delta encoded integer from the underlying stream
	 * @return the number read
	 * @throws IOException if an I/O error occurs 
	 */
	
	public int readDelta() throws IOException {		
		final int msb = readGamma();
		return ( ( 1 << msb ) | readBinary( msb ) ) - 1;
	}
	
	/**
	 * Reads a skewed-golomb encoded integer from the underlying stream
	 * Consider a bucket-vector <code>v = <b, 2b, 4b, ... , 2^i b, ...> </code>
	 * The sum of the elements in the vector goes
	 * 	<code>b, 3b, 7b, 2^(i-1)*b</code>
	 *  
	 * @return the number read
	 * @throws IOException if an I/O error occurs 
	 */	
	public int readSkewedGolomb( final int b ) throws IOException {
		// high element
		final int M = ( ( 1 << readUnary()  ) - 1 ) * b;
		// lower element
		final int m = ( M / ( 2 * b ) ) * b;
		return m + readMinimalBinary( M - m ) ;
	}
	
	/**
	 * Reads a sequence of numbers from the stream interpolative coded.
	 * @param data the result vector
	 * @param offset offset where to write in the vector
	 * @param len the number of integers to decode.
	 * @param lo a lower bound (the same one passed to writeInterpolativeCoding)
	 * @param hi an upper bound (the same one passed to writeInterpolativeCoding)
	 * @throws java.io.IOException if an I/O error occurs
	 */
	public void readInterpolativeCoding( int data[], int offset, int len, int lo, int hi ) throws java.io.IOException {
		final int h, m;
		
		if ( len == 0 ) return;
		if ( len == 1 ) {
			data[ offset ] = readMinimalBinaryZero( hi - lo  ) + lo  ;		
			return;
		}
		
		h = len / 2;
		m = readMinimalBinaryZero( hi - len + h  - ( lo + h ) + 1 ) + lo + h ;
		data[ offset + h ] = m ;
		
		readInterpolativeCoding(  data, offset, h, lo, m - 1 );
		readInterpolativeCoding(  data, offset + h + 1, len - h - 1, m + 1, hi );
	}
	
	/**
	 * Reads a minimal binary encoded number, when the upper bound can b zero.
	 * Used to interpolative code	
	 * @param b the upper bound
	 * @return the int read
	 * @throws IOException if an I/O error occurs
	 */
	public int readMinimalBinaryZero(int b) throws java.io.IOException{
		if(b > 0 ) return readMinimalBinary(b);
		else return 0;
	}
	
	/**
	 * Aligns the stream to the next byte
	 * @throws IOException if an I/O error occurs
	 */
	public void align() throws IOException {
		if ( ( bitOffset & 7 ) == 0 ) return;		
		bitOffset = 0;		
		byteOffset++;		
		byteRead = dis.readByte();		
	}
}
