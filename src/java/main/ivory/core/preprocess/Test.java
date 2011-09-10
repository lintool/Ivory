package ivory.core.preprocess;

public class Test {

  /**
   * @param args
   */
  public static void main(String[] args) {
    String s = "1";
    bytes = new byte[] { (byte) (-3 & 0xff) }; //s.getBytes();
    
    System.out.println((byte) (-3 & 0xff));
    System.out.println(bytes.length);
//    System.out.println(bytes[0]);
//
//    System.out.println(bytes[0] & 0xff);
//
//    bytes[0] = (byte) (bytes[0] & 0xff);
    
//    System.out.println(bytes[0]>>1 & 1);
//    System.out.println(bytes[0]>>2 & 1);
//    System.out.println(bytes[0]>>3 & 1);
//    System.out.println(bytes[0]>>4 & 1);
//    System.out.println(bytes[0]>>5 & 1);
//    System.out.println(bytes[0]>>6 & 1);
//    System.out.println(bytes[0]>>7 & 1);
//    System.out.println(bytes[0]>>8 & 1);

//    for ( int i=0; i<8; i++) {
//      System.out.println((bytes[0]>> i % Byte.SIZE) & 1);
//
//    }
    
    for ( int i=0;i<8;i++) {
      System.out.println(getBoolean(i));
    }
  }

  private static byte[] bytes;

  public static boolean getBoolean( long index ) {
    final int byteIndex = (int)( index / Byte.SIZE );
    final int offset = (int) index % Byte.SIZE;
    //System.out.println("BYTE " + byteIndex + " " + offset);
    //System.out.println(bytes[byteIndex] + " " + ((((bytes[byteIndex] & 0xff) >> offset) & 1) == 0));
    return (((bytes[byteIndex] & 0xff) >> offset) & 1) != 0;
    
    //return ((bytes[byteIndex] & 0xff >> offset) & 1 ) == 0;
  }

}
