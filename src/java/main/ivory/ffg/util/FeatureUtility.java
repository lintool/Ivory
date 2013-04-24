package ivory.ffg.util;

import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.common.io.Files;

import org.xml.sax.SAXException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import ivory.ffg.feature.Feature;
import ivory.ffg.feature.OrderedWindowSequentialDependenceFeature;
import ivory.ffg.feature.TermFeature;
import ivory.ffg.feature.UnorderedWindowSequentialDependenceFeature;

import ivory.ffg.score.BM25ScoringFunction;
import ivory.ffg.score.DirichletScoringFunction;
import ivory.ffg.score.ScoringFunction;
import ivory.ffg.score.TfScoringFunction;
import ivory.ffg.score.TfIdfScoringFunction;

/**
 * Provides auxiliary functions for parsing feature files..
 *
 * @author Nima Asadi
 */
public class FeatureUtility {
  public static Map<String, Feature> parseFeatures(String featurePath)
    throws Exception {
    return FeatureUtility.loadFeatures(Files.newInputStreamSupplier(new File(featurePath)));
  }

  /**
   * Reads a feature set in XML format as follows:
   * &lt;parameters&gt;
   * &lt;feature fid="Feature_ID" featureClass="Feature_class"
   *     scoringFunctionClass="ScoringFunction_class" scoring_function_parameters /&gt;
   * &lt;/parameters&gt;
   *
   * @param featureInputSupplier An input supplier that provides the features
   * @return A map of feature id to features
   */
  public static Map<String, Feature> loadFeatures(InputSupplier<? extends InputStream> featureInputSupplier)
    throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException {
    Preconditions.checkNotNull(featureInputSupplier);

    Map<String, Feature> features = Maps.newTreeMap();
    Document dom = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(featureInputSupplier.getInput());
    NodeList nodeList = dom.getDocumentElement().getElementsByTagName("feature");

    if(nodeList == null) {
      return null;
    }

    for(int i = 0; i < nodeList.getLength(); i++) {
        Element element = (Element) nodeList.item(i);
        String fid = element.getAttribute("id");
        String featureClass = element.getAttribute("featureClass");
        String scoringFunctionClass = element.getAttribute("scoringFunctionClass");
        features.put(fid, createFeature(featureClass, scoringFunctionClass, element));
    }

    return features;
  }

  private static Feature createFeature(String featureClass, String scoringFunctionClass, Element element)
    throws ClassNotFoundException {
    ScoringFunction scoringFunction = null;

    if(scoringFunctionClass.equals(BM25ScoringFunction.class.getName())) {
      float k1 = Float.parseFloat(element.getAttribute("k1"));
      float b = Float.parseFloat(element.getAttribute("b"));
      scoringFunction = new BM25ScoringFunction(k1, b);
    } else if(scoringFunctionClass.equals(DirichletScoringFunction.class.getName())) {
      float mu = Float.parseFloat(element.getAttribute("mu"));
      scoringFunction = new DirichletScoringFunction(mu);
    } else if(scoringFunctionClass.equals(TfScoringFunction.class.getName())) {
      scoringFunction = new TfScoringFunction();
    } else if(scoringFunctionClass.equals(TfIdfScoringFunction.class.getName())) {
      scoringFunction = new TfIdfScoringFunction();
    } else {
      throw new ClassNotFoundException("Scoring function class not found!");
    }


    Feature feature = null;

    if(featureClass.equals(TermFeature.class.getName())) {
      feature = new TermFeature();
    } else if(featureClass.equals(OrderedWindowSequentialDependenceFeature.class.getName())) {
      int w = Integer.parseInt(element.getAttribute("width"));
      feature = new OrderedWindowSequentialDependenceFeature(w);
    } else if(featureClass.equals(UnorderedWindowSequentialDependenceFeature.class.getName())) {
      int w = Integer.parseInt(element.getAttribute("width"));
      feature = new UnorderedWindowSequentialDependenceFeature(w);
    } else {
      throw new ClassNotFoundException("Feature class not found!");
    }

    feature.initialize(scoringFunction);
    return feature;
  }
}
