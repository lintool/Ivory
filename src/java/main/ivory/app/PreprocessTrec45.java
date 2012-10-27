/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

import java.util.Map;

import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.ImmutableMap;

import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

public class PreprocessTrec45 extends PreprocessCollection {
  @Override
  public Map<String, String> getCollectionSettings() {
    return new ImmutableMap.Builder<String, String>()
        .put(IndexBuilder.COLLECTION_PATH_OPTION, collectionPath)
        .put(IndexBuilder.COLLECTION_NAME_OPTION, "TREC_vol45")
        .put(IndexBuilder.INDEX_OPTION, indexPath)
        .put(IndexBuilder.MAPPING_OPTION, TrecDocnoMapping.class.getCanonicalName())
        .put(IndexBuilder.FORMAT_OPTION, TrecDocumentInputFormat.class.getCanonicalName())
        .put(IndexBuilder.TOKENIZER_OPTION, GalagoTokenizer.class.getCanonicalName())
        .put(IndexBuilder.MIN_DF_OPTION, "2")
        .build();
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PreprocessTrec45(), args);
  }
}
