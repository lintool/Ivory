/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

package ivory.core.util;


import ivory.core.exception.ConfigurationException;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Attr;
import org.w3c.dom.DOMConfiguration;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


/**
 * @author Don Metzler
 * 
 */
public class XMLTools {

	/**
	 * @param node
	 * @param name
	 * @param defaultValue
	 */
	public static boolean getAttributeValue(Node node, String name, boolean defaultValue) {
		String value = getAttributeValue(node, name, null);
		if (value == null) {
			setAttributeValue(node, name, defaultValue + "");
			return defaultValue;
		}
		return Boolean.parseBoolean(value);
	}

	/**
	 * @param node
	 * @param name
	 * @param defaultValue
	 */
	public static float getAttributeValue(Node node, String name, float defaultValue) {
		String value = getAttributeValue(node, name, null);
		if (value == null) {
			setAttributeValue(node, name, defaultValue + "");
			return defaultValue;
		}
		return Float.parseFloat(value);
	}
	
	/**
	 * @param node
	 * @param name
	 * @param defaultValue
	 */
	public static int getAttributeValue(Node node, String name, int defaultValue) {
		String value = getAttributeValue(node, name, null);
		if (value == null) {
			setAttributeValue(node, name, defaultValue + "");
			return defaultValue;
		}
		return Integer.parseInt(value);
	}

	public static String getAttributeValue(Node node, String name) {
		if (node == null || !node.hasAttributes()) {
			return null;
		}

		NamedNodeMap attributes = node.getAttributes();
		if (attributes.getNamedItem(name) == null) {
			return null;
		}

		return attributes.getNamedItem(name).getNodeValue();
	}

  /**
   * Returns the value of an attribute, and if the attribute is not found, throws an
   * ConfigurationException with the specified message.
   */
  public static String getAttributeValueOrThrowException(Node node, String name, String errMsg)
      throws ConfigurationException {
    if (node == null || !node.hasAttributes()) {
      throw new ConfigurationException(errMsg);
    }

    NamedNodeMap attributes = node.getAttributes();
    if (attributes.getNamedItem(name) == null) {
      throw new ConfigurationException(errMsg);
    }

    return attributes.getNamedItem(name).getNodeValue();
  }

	/**
	 * @param node
	 * @param name
	 * @param defaultValue
	 */
	public static String getAttributeValue(Node node, String name, String defaultValue) {
		if (node == null || !node.hasAttributes()) {
			return defaultValue;
		}

		NamedNodeMap attributes = node.getAttributes();
		if (attributes.getNamedItem(name) == null) {
			setAttributeValue(node, name, defaultValue);
			return defaultValue;
		}

		return attributes.getNamedItem(name).getNodeValue();
	}

	public static String getStringAttributeValue(Node node, String name) {
		return getAttributeValue(node, name, null);	}
	
	/**
	 * @param node
	 * @param name
	 * @param value
	 */
	public static synchronized void setAttributeValue(Node node, String name, String value) {
		NamedNodeMap attributes = node.getAttributes();
		Attr a = node.getOwnerDocument().createAttribute(name);
		a.setValue(value);
		attributes.setNamedItem(a);
	}

	// Pretty-prints a DOM document to XML using DOM Load and Save's
	// LSSerializer. Note that the "format-pretty-print" DOM configuration
	// parameter can only be set in JDK 1.6+.
	public static String format(String xml) {
		return format(parseXMLString(xml));
	}
	
	public static String format(Document doc) {
		DOMImplementation domImplementation = doc.getImplementation();
		
//		if ( node instanceof Node ) {
//			domImplementation = node.getOwnerDocument().getImplementation();
//		} else if ( node instanceof Document ) {
//			domImplementation = ((Document) node).getImplementation();
//		}
		
		if (domImplementation.hasFeature("LS", "3.0")
				&& domImplementation.hasFeature("Core", "2.0")) {
			DOMImplementationLS domImplementationLS = (DOMImplementationLS) domImplementation
					.getFeature("LS", "3.0");
			LSSerializer lsSerializer = domImplementationLS.createLSSerializer();
			DOMConfiguration domConfiguration = lsSerializer.getDomConfig();
			if (domConfiguration.canSetParameter("format-pretty-print", Boolean.TRUE)) {
				lsSerializer.getDomConfig().setParameter("format-pretty-print", Boolean.TRUE);
				LSOutput lsOutput = domImplementationLS.createLSOutput();
				lsOutput.setEncoding("UTF-8");
				StringWriter stringWriter = new StringWriter();
				lsOutput.setCharacterStream(stringWriter);
				lsSerializer.write(doc, lsOutput);
				return stringWriter.toString();
			} else {
				throw new RuntimeException(
						"DOMConfiguration 'format-pretty-print' parameter isn't settable.");
			}
		} else {
			throw new RuntimeException("DOM 3.0 LS and/or DOM 2.0 Core not supported.");
		}
	}

	private static Document parseXMLString(String in) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			InputSource is = new InputSource(new StringReader(in));
			return db.parse(is);
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		} catch (SAXException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
