package ivory.sqe.querygenerator;

import java.util.List;

public class PhrasePair {
  private List<TokenPair> tokenPairs;
  private String sourcePhrase, targetPhrase;
  private float score;
  
  public PhrasePair(String sourcePhrase, String targetPhrase, String alignments) {
    super();
    this.sourcePhrase = sourcePhrase;
    this.targetPhrase = targetPhrase;
    // use alignments to construct all token pairs
  }

  public String getSource() {
    return sourcePhrase;
  }

  public void setSource(String sourcePhrase) {
    this.sourcePhrase = sourcePhrase;
  }

  public String getTarget() {
    return targetPhrase;
  }

  public void setTarget(String targetPhrase) {
    this.targetPhrase = targetPhrase;
  }
  
  public List<TokenPair> getTokenPairs() {
    return tokenPairs;
  }
  
  public float getScore() {
    return score;
  }

  public boolean isGlue() {
    return (score == -1);
  }
}
