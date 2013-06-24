
package ivory.core.tokenize;

import org.tartarus.snowball.SnowballProgram;

import com.google.common.base.Preconditions;

public class SnowballStemmer implements Stemmer {
  SnowballProgram sb;

  public SnowballStemmer(SnowballProgram p) {
    this.sb = Preconditions.checkNotNull(p);
  }
  
  public String toStem(String token){
    sb.setCurrent(token);
    sb.stem();
    return sb.getCurrent();
  }
}
