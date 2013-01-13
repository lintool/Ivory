/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval research
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.app;

import ivory.core.tokenize.GalagoTokenizer;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.umd.cloud9.collection.medline.MedlineCitationInputFormat;
import edu.umd.cloud9.collection.medline.MedlineDocnoMapping;

public class PreprocessMedline extends PreprocessCollection {
  // Append a few hard-coded arguments and then dispatch to generic superclass.
  public static void main(String[] args) throws Exception {
    Map<String, String> map = new ImmutableMap.Builder<String, String>()
        .put(PreprocessCollection.COLLECTION_NAME, "Medline")
        .put(PreprocessCollection.DOCNO_MAPPING, MedlineDocnoMapping.class.getCanonicalName())
        .put(PreprocessCollection.INPUTFORMAT, MedlineCitationInputFormat.class.getCanonicalName())
        .put(PreprocessCollection.TOKENIZER, GalagoTokenizer.class.getCanonicalName())
        .put(PreprocessCollection.MIN_DF, "2")
        .build();
    
    List<String> s = Lists.newArrayList(args);
    for (Map.Entry<String, String> e : map.entrySet()) {
      s.add("-" + e.getKey());
      s.add(e.getValue());
    }

    ToolRunner.run(new PreprocessMedline(), s.toArray(new String[s.size()]));
  }
}
