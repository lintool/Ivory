package ivory.sqe.querygenerator;

import java.util.Map;
import java.util.Set;

import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.map.HMapSIW;

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
