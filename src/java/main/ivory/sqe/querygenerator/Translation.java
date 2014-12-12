package ivory.sqe.querygenerator;

import java.util.Map;
import java.util.Set;

import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HMapStIW;

public abstract class Translation {
  public static final int NBEST = 0, DERIVATION = 1; 
  private Map<String, HMapStFW> tok2tokDist;
  private Set<String> targetTokens;      // all non-stopword target tokens s.t. aligned to some source non-stopword token
  private HMapStFW targetPhraseDist;     // map from RHSs of rules to translation scores; there is only one RHS (equal to entire translation), if we don't have derivation info
  private HMapStIW sourceTokenCnt;
  private String originalQuery;
  private int count;
  private Map<String,String> stemMapping;
  
  public abstract int getType();

  public Map<String, String> getStemMapping() {
    return stemMapping;
  }

  public void setStemMapping(Map<String, String> stemMapping) {
    this.stemMapping = stemMapping;
  }
  
  public Set<String> getTargetTokens() {
    return targetTokens;
  }

  public void setTargetTokens(Set<String> targetTokens) {
    this.targetTokens = targetTokens;
  }

  public HMapStIW getSourceTokenCnt() {
    return sourceTokenCnt;
  }

  public void setSourceTokenCnt(HMapStIW sourceTokenCnt) {
    this.sourceTokenCnt = sourceTokenCnt;
  }

  public void setPhraseDist(HMapStFW dist) {
    targetPhraseDist = dist;
  }
  
  public HMapStFW getPhraseDist() {
    return targetPhraseDist;
  }

  public String getOriginalQuery() {
    return originalQuery;
  }
  
  public void setOriginalQuery(String o) {
    originalQuery = o;
  }

  public Map<String, HMapStFW> getTokenDist() {
    return tok2tokDist;
  }
  
  public void setTokenDist(Map<String, HMapStFW> dist) {
    tok2tokDist = dist;
  }

  public void setCount(int c) {
    count = c;
  }
  
  public int getCount() {
    return count;
  }

  public HMapStFW getDistributionOf(String srcToken) {
    return tok2tokDist.get(srcToken);
  }
}
