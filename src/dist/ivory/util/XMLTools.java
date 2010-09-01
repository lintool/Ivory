/**
 * 
 */
package ivory.util;

import org.w3c.dom.Attr;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

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
	public static boolean getAttributeValue ( Node node, String name, boolean defaultValue ) {
		String value = getAttributeValue( node, name, null );
		if( value == null ) {
			setAttributeValue( node, name, defaultValue + "" );
			return defaultValue;
		}
		return Boolean.parseBoolean( value );
	}

	/**
	 * @param node
	 * @param name
	 * @param defaultValue
	 */
	public static double getAttributeValue ( Node node, String name, double defaultValue ) {
		String value = getAttributeValue( node, name, null );
		if( value == null ) {
			setAttributeValue( node, name, defaultValue + "");
			return defaultValue;
		}
		return Double.parseDouble( value );
	}

	/**
	 * @param node
	 * @param name
	 * @param defaultValue
	 */
	public static int getAttributeValue ( Node node, String name, int defaultValue ) {
		String value = getAttributeValue( node, name, null );
		if( value == null ) {
			setAttributeValue( node, name, defaultValue + "");
			return defaultValue;
		}
		return Integer.parseInt( value );
	}

	/**
	 * @param node
	 * @param name
	 * @param defaultValue
	 */
	public static String getAttributeValue ( Node node, String name, String defaultValue ) {
		if( node == null || !node.hasAttributes() ) {
			return defaultValue;
		}
	
		NamedNodeMap attributes = node.getAttributes();
		if( attributes.getNamedItem( name ) == null ) {
			setAttributeValue( node, name, defaultValue);
			return defaultValue;
		}
		
		return attributes.getNamedItem( name ).getNodeValue();
	}

	/**
	 * @param node
	 * @param name
	 * @param value
	 */
	public static synchronized void setAttributeValue( Node node, String name, String value ) {
		NamedNodeMap attributes = node.getAttributes();
		Attr a = node.getOwnerDocument().createAttribute( name );
		a.setValue( value );
		attributes.setNamedItem( a );
	}
}
