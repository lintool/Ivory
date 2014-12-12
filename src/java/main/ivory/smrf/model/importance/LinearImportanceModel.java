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

package ivory.smrf.model.importance;

import ivory.core.ConfigurationException;
import ivory.core.RetrievalException;
import ivory.core.util.XMLTools;
import ivory.smrf.model.Clique;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import tl.lin.data.map.HMapKF;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author Don Metzler
 */
public class LinearImportanceModel extends ConceptImportanceModel {

  // MetaFeatures.
  private final List<MetaFeature> metafeatures = Lists.newArrayList();

  // MetaFeature values.
  private final Map<MetaFeature, HMapKF<String>> metafeatureValues = Maps.newHashMap();

  // Default feature values for each meta feature.
  private final HMapKF<String> defaultValues = new HMapKF<String>();

  public void configure(Node model) throws ConfigurationException {
    // Clear meta-feature data.
    metafeatures.clear();
    metafeatureValues.clear();
    defaultValues.clear();

    // Construct MRF feature by feature.
    NodeList children = model.getChildNodes();

    float totalMetaFeatureWeight = 0.0f;
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);

      if ("feature".equals(child.getNodeName())) {

        // collection_freq, document_freq, clue_cf, or enwiki_cf
        String metaFeatureName = XMLTools.getAttributeValue(child, "id", "");
        float metaFeatureWeight = XMLTools.getAttributeValue(child, "weight", -1.0f);

        if (metaFeatureName == "" || metaFeatureWeight == -1) {
          throw new ConfigurationException("Must specify metafeature name and weight.");
        }

        MetaFeature mf = new MetaFeature(metaFeatureName, metaFeatureWeight);
        metafeatures.add(mf);

        totalMetaFeatureWeight += metaFeatureWeight;

        String file = XMLTools.getAttributeValue(child, "file", null);
        if (file == null) {
          throw new ConfigurationException(
              "Must specify the location of the metafeature stats file.");
        }

        try {
          metafeatureValues.put(mf, readDataStats(file));
        } catch (IOException e) {
          throw new RetrievalException("Error: " + e);
        }

        float defaultValue = XMLTools.getAttributeValue(child, "default", 0.0f);
        defaultValues.put(mf.getName(), defaultValue);
      }
    }

    // Normalize meta feature weights.
    for (int i = 0; i < metafeatures.size(); i++) {
      MetaFeature mf = (MetaFeature) metafeatures.get(i);
      float w = mf.getWeight() / totalMetaFeatureWeight;
      mf.setWeight(w);
    }
  }

  @Override
  public float getConceptWeight(String concept) {
    // Compute query-dependent clique weight.
    float weight = 0.0f;
    for (MetaFeature mf : metafeatures) {
      float metaWeight = mf.getWeight();
      float cliqueFeatureVal = computeFeatureValue(concept, mf);
      weight += metaWeight * cliqueFeatureVal;
    }

    return weight;
  }

  @Override
  public float getCliqueWeight(Clique c) {
    return getConceptWeight(c.getConcept());
  }

  public float computeFeatureValue(String cliqueTerms, MetaFeature f) {
    float count;

    // Get meta-feature values for f.
    HMapKF<String> mfValues = metafeatureValues.get(f);

    // Look up value for clique terms.
    if (mfValues != null && mfValues.containsKey(cliqueTerms)) {
      count = mfValues.get(cliqueTerms);
    } else {
      count = defaultValues.get(f.getName());
    }

    return count;
  }

  public List<MetaFeature> getMetaFeatures() {
    return metafeatures;
  }

  // Reads MetaFeature statistics from a file,
  public static HMapKF<String> readDataStats(String file) throws IOException {
    Configuration conf = new Configuration();
    HMapKF<String> values = new HMapKF<String>();

    FileSystem fs = FileSystem.get(conf);
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));

    String line;
    while ((line = in.readLine()) != null) {
      String[] tokens = line.split("\t");

      String concept = tokens[0];
      float value = Float.parseFloat(tokens[1]);

      values.put(concept, value);
    }

    return values;
  }
}
