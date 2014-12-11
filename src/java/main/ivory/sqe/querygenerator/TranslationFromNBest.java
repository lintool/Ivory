package ivory.sqe.querygenerator;

import java.util.Map;
import java.util.Set;

import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HMapStIW;

public class TranslationFromNBest extends Translation {
  
   public TranslationFromNBest(int n, String origQuery, Map<String,String> stemmed2stemmed, Set<String> bagOfTargetTokens, Map<String, HMapStFW> token2tokenDist, HMapStFW phraseDist, HMapStIW srcTokenCnt) {
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
