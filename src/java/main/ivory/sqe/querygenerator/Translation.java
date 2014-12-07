package ivory.sqe.querygenerator;

import java.util.Map;
import java.util.Set;

import tl.lin.data.map.HMapSFW;
import tl.lin.data.map.HMapSIW;

public abstract class Translation {
  public static final int NBEST = 0, DERIVATION = 1; 
  private Map<String, HMapSFW> tok2tokDist;
  private Set<String> targetTokens;      // all non-stopword target tokens s.t. aligned to some source non-stopword token
  private HMapSFW targetPhraseDist;     // map from RHSs of rules to translation scores; there is only one RHS (equal to entire translation), if we don't have derivation info
  private HMapSIW sourceTokenCnt;
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

  public HMapSIW getSourceTokenCnt() {
    return sourceTokenCnt;
  }

  public void setSourceTokenCnt(HMapSIW sourceTokenCnt) {
    this.sourceTokenCnt = sourceTokenCnt;
  }

  public void setPhraseDist(HMapSFW dist) {
    targetPhraseDist = dist;
  }
  
  public HMapSFW getPhraseDist() {
    return targetPhraseDist;
  }

  public String getOriginalQuery() {
    return originalQuery;
  }
  
  public void setOriginalQuery(String o) {
    originalQuery = o;
  }

  public Map<String, HMapSFW> getTokenDist() {
    return tok2tokDist;
  }
  
  public void setTokenDist(Map<String, HMapSFW> dist) {
    tok2tokDist = dist;
  }

  public void setCount(int c) {
    count = c;
  }
  
  public int getCount() {
    return count;
  }

  public HMapSFW getDistributionOf(String srcToken) {
    return tok2tokDist.get(srcToken);
  }
}
