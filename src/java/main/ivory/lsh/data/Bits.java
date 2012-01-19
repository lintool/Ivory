package ivory.lsh.data;
/*
	Copyright (c) 2004 Pablo Bleyer Kocik.

	Redistribution and use in source and binary forms, with or without
	modification, are permitted provided that the following conditions are met:

	1. Redistributions of source code must retain the above copyright notice, this
	list of conditions and the following disclaimer.

	2. Redistributions in binary form must reproduce the above copyright notice,
	this list of conditions and the following disclaimer in the documentation
	and/or other materials provided with the distribution.

	3. The name of the author may not be used to endorse or promote products
	derived from this software without specific prior written permission.

	THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
	WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
	MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
	EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
	SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
	PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
	BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
	IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
	POSSIBILITY OF SUCH DAMAGE.
 */


/**
	Bits is a bit-packet abstraction, containing a number of bits
	inside a byte array
 */
public class Bits {

  /** Bits are stored in a byte array, lsb to msb */
  public byte[] bits;
  /** The actual number of bits in this packet */
  public int length;

  /** Reverse a string */
  public static String reverse(String s) {
    //		int l = s.length();
    //		StringBuffer b = new StringBuffer();
    //		for (int i=0; i<l; ++i) b.append(s.charAt(l-i-1));
    StringBuffer b = new StringBuffer(s);
    return b.reverse().toString();
  }

  /** Reverse bits in a packet */
  public static Bits reverse(Bits b) {
    byte[] nb = new byte[b.length];
    for (int i=0; i<b.length; ++i)
      if ( ( b.bits[i/8] & (1<<(i%8)) ) != 0 )
        nb[ (b.length-1-i)/8 ] |= 1<<( (b.length-1-i)%8 );
    return new Bits(nb, b.length);
  }

  /** Create new Bits object from existing bytes and length (number of bits) */
  public Bits(byte[] b, int l) {
    bits = b;
    length = l;
  }

  /** Helper to create a Bits object from existing byte array containing bits */
  public static Bits asBits(byte[] b, int l) {
    return new Bits(b, l);
  }

  /** Helper to create a String object from existing byte array containing bits */
  public static String asString(byte[] b, int l) {
    return (new Bits(b, l)).toString();
  }

  /** Create new Bits object from String (bits are read right to left, lsb to msb) */
  public Bits(String s) {
    length = s.length();
    int r = length/8, m = length%8;
    if (m != 0) bits = new byte[r+1];
    else bits = new byte[r];

    for (int i=0; i<length; ++i) {
      int f = (s.charAt(length-i-1) == '0') ? 0 : 1;
      f = f<<(7-i%8);
      bits[i/8] |= f;
    }
  }

  /** Create new Bits object of constant value (0/1) and specified length */
  public Bits(int c, int l) {
    length = l;
    int r = length/8, m = length%8;
    if (m != 0) bits = new byte[r+1];
    else bits = new byte[r];

    for (int i=0; i<bits.length; ++i)
      bits[i] = (c == 0) ? 0 : (byte)0xff;
  }

  /** Helper to create a constant-valued Bits object */
  public static Bits asBits(int c, int l) {
    return new Bits(c, l);
  }

  /** Helper to create a String object with constant value bits */
  public static String asString(int c, int l) {
    return (new Bits(c, l)).toString();
  }

  /** Set bit to value */
  public void setBit(int p, int v) {
    if (v == 0) bits[p/8] &= ~(1<<(7-p%8)); // clear
    else bits[p/8] |= 1<<(7-p%8); // set
  }

  /** Get bit value */
  public int getBit(int p) {
    return ((bits[p/8]>>(7-p%8)) & 1);
  }

  /** Set a range of contiguous bits (up to 32) to a value.
		Note that range is inclusive and, if reversed, the value is reversed.
   */
  public void setBits(int e, int b, int v) {
    if (b >= length || e >= length) throw new IllegalArgumentException();
    if (b <= e) // not reversed
      for (int i=0; i<=(e-b); ++i) {
        int p = b+i;
        int w = v & (1<<i);
        if (w == 0) bits[p/8] &= ~(1<<(p%8)); // clear
        else bits[p/8] |= 1<<(p%8); // set
      }
    else // reversed
      for (int i=0; i<=(b-e); ++i) {
        int p = b-i;
        int w = v & (1<<i);
        if (w == 0) bits[p/8] &= ~(1<<(p%8)); // clear
        else bits[p/8] |= 1<<(p%8); // set
      }
  }

  /** Set value of a range of contiguous bits (high/low encoded in a single int) */
  public void setBits(int r, int v) {
    int ra = Math.abs(r), rb = ra & 0xffff, re = (ra>>16) & 0xffff;
    if (re == 0) re = rb; // single bit
    if (r<0) setBits(rb, re, v); // reversed
    else setBits(re, rb, v); // normal
  }

  /** Get value of a range of contiguous bits as an int */
  public int getBits(int e, int b) {
    if (b >= length || e >= length) throw new IllegalArgumentException();

    int r = 0;
    if (b <= e) // not reversed
      for (int i=0; i<=(e-b); ++i) {
        int p = b+i;
        if ((bits[p/8] & (1<<(p%8))) != 0) r |= 1<<i;
      }
    else // reversed
      for (int i=0; i<=(b-e); ++i) {
        int p = b-i;
        if ((bits[p/8] & (1<<(p%8))) != 0) r |= 1<<i;
      }
    return r;
  }

  /** Get value of a range of contiguous bits (high/low position encoded) */
  public int getBits(int r) {
    int ra = Math.abs(r), rb = ra & 0xffff, re = (ra>>16) & 0xffff;
    if (re == 0) re = rb; // single bit
    if (r<0) return getBits(rb, re); // reversed
    else return getBits(re, rb); // normal
  }

  /** Get sub-Bits */
  public Bits extract(int e, int b) {
    if (b >= length || e >= length) throw new IllegalArgumentException();

    int l = 1 + ((b < e) ? e-b : b-e);
    int r = l/8, m = l%8;
    byte[] q = new byte[(m==0) ? r : r+1];

    if (b <= e) // not reversed
      for (int i=0; i<l; i++) {
        int p = b+i;
        if (((bits[p/8]>>(7-p%8)) & 1) != 0) q[i/8] |= 1<<(7-i%8);
      }
    //		else // reversed
    //			for (int i=0; i<l; ++i) {
    //				int p = b-i;
    //				if ((bits[p/8] & (1<<(p%8))) != 0) q[i/8] |= 1<<(i%8);
    //			}
    return new Bits(q, l);
  }

  /** Append bit packets */
  public Bits append(Bits b) {
    int l = length + b.length;
    int r = l/8, m = l%8;
    byte[] q = new byte[(m==0) ? r : r+1];

    for (int i=0; i<b.length; ++i)
      if ((b.bits[i/8] & (1<<(7-i%8))) != 0) q[i/8] |= 1<<(7-i%8);
    for (int i=0; i<length; ++i) {
      int p = i + b.length;
      if ((bits[i/8] & (1<<(7-i%8))) != 0) q[p/8] |= 1<<(7-p%8);
    }
    return new Bits(q, l);
  }

  /** Reverse bits */
  public void reverse() {
    //		byte[] nb = new byte[length];
    //		for (int i=0; i<length; ++i)
    //			if ((bits[i/8] & (1<<(i%8))) != 0) nb[(length-1-i)/8] |= 1<<((length-1-i)%8);
    //		bits = nb;
    bits = Bits.reverse(this).bits;
  }

  public String toString() {
    StringBuffer b = new StringBuffer();
    for (int i=0; i<length; ++i) {
      if (getBit(i) == 0) b.append('0');
      else b.append('1');
    }
    return b.toString();
  }

  /** Test class */
  public static void main(String[] args) {
    String s = "00001000100101";
    System.out.println("s:" + s);
    System.out.println("s':" + reverse(s));

    Bits b = new Bits(s);
    System.out.println("b:" + b.toString());
    b.setBit(0, 0);
    System.out.println("b:" + b.toString());
    b.setBit(1, 1);
    System.out.println("b:" + b.toString());
    b.reverse();
    System.out.println("b:" + b.toString());

    Bits c = b.extract(10, 3);
    System.out.println("c:" + c.toString());
    c = b.extract(3, 10);
    System.out.println("c:" + c.toString());

    b.setBits(7, 0, 0xca);
    System.out.println("b:" + b.toString());
    System.out.println("b[7:0]:" + Integer.toHexString(b.getBits(7, 0)));


    b.setBits(-((7<<16)|0), 0xca);
    System.out.println("b:" + b.toString());
    System.out.println("b[0:7]:" + Integer.toHexString(b.getBits(0, 7)));

    b = b.append(c);
    System.out.println("b+c:" + b.toString());

    b = new Bits(0, 20);
    System.out.println("b:" + b.toString());
    b = new Bits(1, 20);
    System.out.println("b:" + b.toString());

  }
}
