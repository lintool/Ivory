package ivory.core.data.dictionary;

import it.unimi.dsi.bits.AbstractBitVector;
import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.TransformationStrategy;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

public class DictionaryTransformationStrategy implements TransformationStrategy<CharSequence>, Serializable {
  private static final long serialVersionUID = 1L;
  /** Whether we should guarantee prefix-freeness by adding 0 to the end of each string. */
  private final boolean prefixFree;

  /** Creates an ISO transformation strategy. The strategy will map a string to the lowest eight bits of its natural UTF16 bit sequence.
   * 
   * @param prefixFree if true, the resulting set of binary words will be made prefix free by adding 
   */
  public DictionaryTransformationStrategy( boolean prefixFree ) {
    this.prefixFree = prefixFree;
  }
  
  public static class ISOCharSequenceBitVector extends AbstractBitVector implements Serializable {
    private static final long serialVersionUID = 1L;
    private transient CharSequence s;
    private transient long length;
    private transient long actualEnd;
    byte[] bytes;

    public ISOCharSequenceBitVector( final CharSequence s, final boolean prefixFree) {
      this.s = s;
      try {
        bytes = s.toString().getBytes("UTF16");
      } catch (UnsupportedEncodingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      actualEnd = bytes.length * Byte.SIZE;
      //actualEnd = s.length() * Byte.SIZE;
      length = actualEnd + ( prefixFree ? Byte.SIZE * 2: 0 );
      //System.out.println(toString() + " " + s);
    }
    
    public boolean getBoolean( long index ) {
      if ( index > length ) throw new IndexOutOfBoundsException();
      if ( index >= actualEnd ) return false;
      final int byteIndex = (int)( index / Byte.SIZE );
      //return ( (bytes[byteIndex] & 0x80) >>> index % Byte.SIZE ) != 0; 

      return ( bytes[byteIndex] & 0x80 >>> index % Byte.SIZE ) != 0; 
    }
    
    public long length() {
      return length;
    }
  }

  public BitVector toBitVector( final CharSequence s ) {
    return new ISOCharSequenceBitVector( s, prefixFree );
  }

  public long numBits() { return 0; }

  public TransformationStrategy<CharSequence> copy() {
    return this;
  }
  
//  private Object readResolve() {
//    return prefixFree ? PREFIX_FREE_ISO : ISO; 
//  }
}
