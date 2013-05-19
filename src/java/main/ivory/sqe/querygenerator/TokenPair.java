package ivory.sqe.querygenerator;

public class TokenPair {
  private String sourceToken, targetToken;

  public TokenPair(String sourceToken, String targetToken) {
    super();
    this.sourceToken = sourceToken;
    this.targetToken = targetToken;
  }

  public String getSource() {
    return sourceToken;
  }

  public void setSource(String sourceToken) {
    this.sourceToken = sourceToken;
  }

  public String getTarget() {
    return targetToken;
  }

  public void setTarget(String targetToken) {
    this.targetToken = targetToken;
  }
  
  public boolean isSourceNull() {
    return sourceToken.equals("NULL");
  }
}
