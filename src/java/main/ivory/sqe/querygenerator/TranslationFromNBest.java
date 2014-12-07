package ivory.sqe.querygenerator;

import java.util.Map;
import java.util.Set;

import tl.lin.data.map.HMapSFW;
import tl.lin.data.map.HMapSIW;

public class TranslationFromNBest extends Translation {
  
   public TranslationFromNBest(int n, String origQuery, Map<String,String> stemmed2stemmed, Set<String> bagOfTargetTokens, Map<String, HMapSFW> token2tokenDist, HMapSFW phraseDist, HMapSIW srcTokenCnt) {
     setOriginalQuery(origQuery);
     setPhraseDist(phraseDist);
     setTokenDist(token2tokenDist);
     setCount(n);
     setSourceTokenCnt(srcTokenCnt);
     setTargetTokens(bagOfTargetTokens);
     setStemMapping(stemmed2stemmed);
  }

  @Override
  public int getType() {
    return DERIVATION;
  }
}
