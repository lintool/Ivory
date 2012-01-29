package ivory.ltr.operator;

import java.io.InputStream;
import java.io.IOException;
import java.io.File;
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

/**
 * Provides auxiliary functions to parse operators.
 *
 * @author Nima Asadi
 */
public class OperatorUtil {
  /**
   * Loads and tokenizes a set of features
   *
   * @param  featurePath Path to the file containing the feature descriptions
   * @return Map of feature id to operator
   */
  public static Map<String, Operator> parseOperators(String featurePath)
      throws Exception {
    return OperatorUtil.loadOperators(Files.newInputStreamSupplier(new File(featurePath)));
  }

  /**
   * Reads a feature set in an XML format as follows:
   *
   * @param featureInputSupplier An input supplier that provides the feature descriptions
   * @return A map of feature id to Operator
   */
  public static Map<String, Operator> loadOperators(InputSupplier<? extends InputStream> featureInputSupplier)
    throws ParserConfigurationException, SAXException, IOException, Exception {
    Preconditions.checkNotNull(featureInputSupplier);

    Map<String, Operator> operators = Maps.newHashMap();
    Document dom = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(featureInputSupplier.getInput());
    NodeList nodeList = dom.getDocumentElement().getElementsByTagName("model");

    if(nodeList == null) {
      return null;
    }

    for(int i = 0; i < nodeList.getLength(); i++) {
      Element element = (Element) nodeList.item(i);
      String modelName = element.getAttribute("id");
      NodeList featureList = element.getElementsByTagName("feature");
      for(int j = 0; j < featureList.getLength(); j++) {
        Element felement = (Element) featureList.item(j);
        String fid = modelName + "-" + felement.getAttribute("id");
        if(felement.hasAttribute("operator")) {
          String className = felement.getAttribute("operator");
          operators.put(fid, (Operator) Class.forName(className).newInstance());
        } else {
          operators.put(fid, new Sum());
        }
      }
    }
    return operators;
  }
}
