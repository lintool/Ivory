package edu.stanford.nlp.util;

/**
 * The goal of this class is to make it easier to load stuff by
 * reflection.  You can hide all of the ugly exception catching, etc
 * by using the static methods in this class.
 *
 * @author John Bauer
 * @author Gabor Angeli (changed)
 */

public class ReflectionLoading {
  // static methods only
  private ReflectionLoading() {}

  /**
   * You can use this as follows:
   * <br>
   *  String s = 
   *    ReflectionLoading.loadByReflection("java.lang.String", "foo");
   * <br>
   *  String s =
   *    ReflectionLoading.loadByReflection("java.lang.String");
   * <br>
   * Note that this uses generics for convenience, but this does
   * nothing for compile-time error checking.  You can do
   * <br>
   *  Integer i = 
   *    ReflectionLoading.loadByReflection("java.lang.String");
   * <br>
   * and it will compile just fine, but will result in a ClassCastException.
   */
   @SuppressWarnings("unchecked")
  static public <T> T loadByReflection(String className, 
                                       Object ... arguments) {    
    try{
      return (T) new MetaClass(className).createInstance(arguments);
    } catch(Exception e){
      throw new ReflectionLoadingException(e);
    }
  }

  /**
   * This class encapsulates all of the exceptions that can be thrown
   * when loading something by reflection.
   */
  static public class ReflectionLoadingException extends RuntimeException {
    public ReflectionLoadingException(Throwable reason) {
      super(reason);
    }
  }
}
