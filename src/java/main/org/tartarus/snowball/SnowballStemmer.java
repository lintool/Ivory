
package org.tartarus.snowball;

import ivory.core.tokenize.Stemmer;

public abstract class SnowballStemmer extends SnowballProgram implements Stemmer {
  public abstract boolean stem();
  public String toStem(String token){
    setCurrent(token);
    stem();
    return getCurrent();
  }
}
