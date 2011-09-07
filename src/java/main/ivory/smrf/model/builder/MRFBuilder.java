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

package ivory.smrf.model.builder;

import java.io.IOException;

import ivory.cascade.model.builder.CascadeFeatureBasedMRFBuilder;
import ivory.core.RetrievalEnvironment;
import ivory.core.exception.ConfigurationException;
import ivory.core.exception.RetrievalException;
import ivory.core.util.XMLTools;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.constrained.GreedyConstrainedMRFBuilder;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 */
public abstract class MRFBuilder {
  protected final RetrievalEnvironment env;

  public MRFBuilder(RetrievalEnvironment env) {
    this.env = Preconditions.checkNotNull(env);
  }

  public abstract MarkovRandomField buildMRF(String[] queryTerms) throws ConfigurationException;

  public static MRFBuilder get(RetrievalEnvironment env, Node model) throws ConfigurationException {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(model);

    // Get model type.
    String modelType = XMLTools.getAttributeValueOrThrowException(model, "type",
        "Model type must be specified!");

    // Build the builder.
    MRFBuilder builder = null;

    try {
      if ("Feature".equals(modelType)) {
        builder = new FeatureBasedMRFBuilder(env, model);
      } else if ("GreedyConstrained".equals(modelType)) {
        builder = new GreedyConstrainedMRFBuilder(env, model);
      } else if (modelType.equals("New")) {
        builder = new CascadeFeatureBasedMRFBuilder(env, model);
      } else {
        throw new ConfigurationException("Unrecognized model type: " + modelType);
      }
    } catch (IOException e) {
      throw new RetrievalException("Error getting MRFBuilder: " + e);
    }

    return builder;
  }
}
