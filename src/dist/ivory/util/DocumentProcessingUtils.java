/**
 * 
 */
package ivory.util;

import ivory.data.PrefixEncodedTermIDMapWithIndex;
import ivory.data.PrefixEncodedTermSet;
import ivory.data.TermDocVector;
import ivory.data.TermDocVectorReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.util.ArrayListOfInts;


/**
 * @author Tamer
 *
 */
public class DocumentProcessingUtils {
	private static final Logger sLogger = Logger.getLogger(DocumentProcessingUtils.class);
	static{
		sLogger.setLevel(Level.WARN);
	}
	public static short TF_CUT = Short.MAX_VALUE;
	//public static short TF_CUT = 5;
	
	public static int getDocLengthFromPositionsMap(Map<String, ArrayListOfInts> termPositionsMap){
		int dl = 0;
		for (Map.Entry<String, ArrayListOfInts> e : termPositionsMap.entrySet()) {
			dl += e.getValue().size();
		}
		return dl;
	}
	
	public static int getDocLengthFromTFMap(Map<String, Short> termTFMap){
		int dl = 0;
		for (Map.Entry<String, Short> e : termTFMap.entrySet()) {
			dl += e.getValue().shortValue();
		}
		return dl;
	}
	
	public static Map<String, ArrayListOfInts> getTermPositionsMap(Indexable doc, Tokenizer mTokenizer){
		// for storing the positions
		Map<String, ArrayListOfInts> positions = new HashMap<String, ArrayListOfInts>();
		
		String text = doc.getContent();
		String[] terms = mTokenizer.processContent(text);

		// the tokenizer may return
		// terms with zero length (empty terms), and the tf may exceed the
		// capacity of a short (in which case we need to handle separately).
		// The doc length and contribution to term count is computed as the
		// sum of all tfs of indexed terms a bit later.

		for (int i = 0; i < terms.length; i++) {
			String term = terms[i];

			// guard against bad tokenization
			if (term.length() == 0)
				continue;

			if (term.length() >= Byte.MAX_VALUE)
				continue;
	
			// remember, token position is numbered started from one...
			if (positions.containsKey(term)) {
				positions.get(term).add(i + 1);
			} else {
				ArrayListOfInts l = new ArrayListOfInts();
				l.add(i + 1);
				positions.put(term, l);
			}
		}

		Iterator<Map.Entry<String, ArrayListOfInts>> it = positions.entrySet().iterator();
		Map.Entry<String, ArrayListOfInts> e;
		ArrayListOfInts positionsList;
		while(it.hasNext()){
			e = it.next();
			positionsList = e.getValue(); // positions.get(e.getKey());
			
			// we're storing tfs as shorts, so check for overflow...
			if (positionsList.size() >= TF_CUT) {
				// There are a few ways to handle this... If we're getting
				// such a high tf, then it most likely means that this is a
				// junk doc. The current implementation simply skips this
				// posting...
				sLogger.warn("Error: tf of " + e.getValue()
						+ " will overflow max short value. docno=" + doc.getDocid() + ", term="
						+ e.getKey());
				//continue;
				it.remove();
				
			}
			positionsList.trimToSize();
		}
		return positions;
	}

	public static TreeMap<Integer, ArrayListOfInts> getTermIDsPositionsMap(Indexable doc, Tokenizer mTokenizer, PrefixEncodedTermIDMapWithIndex termIDMap){
		// for storing the positions
		Map<String, ArrayListOfInts> strPositions = new HashMap<String, ArrayListOfInts>();
		
		String text = doc.getContent();
		String[] terms = mTokenizer.processContent(text);

		// the tokenizer may return
		// terms with zero length (empty terms), and the tf may exceed the
		// capacity of a short (in which case we need to handle separately).
		// The doc length and contribution to term count is computed as the
		// sum of all tfs of indexed terms a bit later.

		for (int i = 0; i < terms.length; i++) {
			String term = terms[i];

			// guard against bad tokenization
			if (term.length() == 0)
				continue;

			if (term.length() >= Byte.MAX_VALUE)
				continue;
	
			// remember, token position is numbered started from one...
			if (strPositions.containsKey(term)) {
				strPositions.get(term).add(i + 1);
			} else {
				ArrayListOfInts l = new ArrayListOfInts();
				l.add(i + 1);
				strPositions.put(term, l);
			}
		}

		// for storing the positions
		TreeMap<Integer, ArrayListOfInts> positions = new TreeMap<Integer, ArrayListOfInts>();
		
		Iterator<Map.Entry<String, ArrayListOfInts>> it = strPositions.entrySet().iterator();
		Map.Entry<String, ArrayListOfInts> e;
		ArrayListOfInts positionsList;
		int id;
		while(it.hasNext()){
			e = it.next();
			positionsList = e.getValue(); // positions.get(e.getKey());
			
			// we're storing tfs as shorts, so check for overflow...
			if (positionsList.size() >= TF_CUT) {
				// There are a few ways to handle this... If we're getting
				// such a high tf, then it most likely means that this is a
				// junk doc. The current implementation simply skips this
				// posting...
				sLogger.warn("Error: tf of " + e.getValue()
						+ " will overflow max short value. docno=" + doc.getDocid() + ", term="
						+ e.getKey());
				continue;
			}
			id = termIDMap.getID(e.getKey());
			if(id <= 0)	continue;
			positions.put(id, positionsList);
		}
		return positions;
	}

	public static TreeMap<Integer, int[]> getTermIDsPositionsMap(TermDocVector doc, PrefixEncodedTermIDMapWithIndex termIDMap){
		// for storing the positions
		Map<String, int[]> strPositions = new HashMap<String, int[]>();
		
		TermDocVectorReader r = null;
		try {
			r = doc.getDocVectorReader();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			throw new RuntimeException("Error getting TermDocVectorReader: "+e1.getMessage());
		}
		String term;
		int[] tp;
		while(r.hasMoreTerms()){
			term = r.nextTerm();
			//TermPositions tp = new TermPositions();
			tp = r.getPositions();
			strPositions.put(term, tp);
		}
		// for storing the positions
		TreeMap<Integer, int[]> positions = new TreeMap<Integer, int[]>();
		
		Iterator<Map.Entry<String, int[]>> it = strPositions.entrySet().iterator();
		Map.Entry<String, int[]> e;
		int[] positionsList;
		int id;
		while(it.hasNext()){
			e = it.next();
			positionsList = e.getValue(); // positions.get(e.getKey());
			id = termIDMap.getID(e.getKey());
			if(id <= 0)	continue;
			positions.put(id, positionsList);
		}
		return positions;
	}

	public static TreeMap<String, ArrayListOfInts> getTermPositionsMap(TermDocVector doc, PrefixEncodedTermSet terms){
		// for storing the positions
		TreeMap<String, ArrayListOfInts> strPositions = new TreeMap<String, ArrayListOfInts>();
		
		TermDocVectorReader r = null;
		try {
			r = doc.getDocVectorReader();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			throw new RuntimeException("Error getting TermDocVectorReader: "+e1.getMessage());
		}
		String term;
		int[] tp;
		while(r.hasMoreTerms()){
			term = r.nextTerm();
			//TermPositions tp = new TermPositions();
			tp = r.getPositions();
			if(terms.getIndex(term)>0)
				strPositions.put(term, new ArrayListOfInts(tp));
		}
		return strPositions;
	}

	public static TreeMap<String, ArrayListOfInts> getTermPositionsMap(TermDocVector doc, Set<String> terms){
		// for storing the positions
		TreeMap<String, ArrayListOfInts> strPositions = new TreeMap<String, ArrayListOfInts>();
		
		TermDocVectorReader r = null;
		try {
			r = doc.getDocVectorReader();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			throw new RuntimeException("Error getting TermDocVectorReader: "+e1.getMessage());
		}
		String term;
		int[] tp;
		while(r.hasMoreTerms()){
			term = r.nextTerm();
			//TermPositions tp = new TermPositions();
			tp = r.getPositions();
			if(terms.contains(term))
				strPositions.put(term, new ArrayListOfInts(tp));
		}
		return strPositions;
	}

	
	//private static HashSet<String> excludedTerms = new HashSet<String>();
	
	public static Map<String, Short> getTermTFMap(Indexable doc, Tokenizer mTokenizer){
		// for storing the positions
		Map<String, Short> termTFMap = new TreeMap<String, Short>();
		
		String text = doc.getContent();
		String[] terms = mTokenizer.processContent(text);

		// It may be tempting to compute doc length and and contribution to
		// collection term count here, but this may potentially result
		// in inaccurate numbers for a few reasons: the tokenizer may return
		// terms with zero length (empty terms), and the tf may exceed the
		// capacity of a short (in which case we need to handle separately).
		// The doc length and contribution to term count is computed as the
		// sum of all tfs of indexed terms a bit later.

		Short tf;
		Short one = new Short((short)1);
		for (int i = 0; i < terms.length; i++) {
			String term = terms[i];

			// guard against bad tokenization
			if (term.length() == 0)
				continue;

			if (term.length() >= Byte.MAX_VALUE)
				continue;
	
			tf = termTFMap.get(term);
			if(tf == null)
				termTFMap.put(term, one);
			else{
				if(tf.intValue() < TF_CUT)
					termTFMap.put(term, new Short((short)(tf.shortValue()+(short)1)));
			}
		}

		Iterator<Map.Entry<String, Short>> it = termTFMap.entrySet().iterator();
		Map.Entry<String, Short> e;
		while(it.hasNext()){
			e = it.next();
			// we're storing tfs as shorts, so check for overflow...
			if (e.getValue().intValue() >= TF_CUT) {
				// There are a few ways to handle this... If we're getting
				// such a high tf, then it most likely means that this is a
				// junk doc. The current implementation simply skips this
				// posting...
				sLogger.warn("Error: tf of " + e.getValue()
						+ " will overflow max short value. docno=" + doc.getDocid() + ", term="
						+ e.getKey());
				//continue;
				it.remove();
			}

		}
		return termTFMap;
	}
	
	public static void printTermPositionsMap(Map<String, ArrayListOfInts> termPositionsMap){
		for (Map.Entry<String, ArrayListOfInts> e : termPositionsMap.entrySet()) {
			System.out.print("("+e.getKey()+":");
			ArrayListOfInts positionsList = e.getValue(); // positions.get(e.getKey());
			for(int i : positionsList.getArray())
				System.out.print(" "+i);
			System.out.print(")");
		}
		System.out.println();
	}
	
	public static void printTermTFMap(Map<String, Short> termTFMap){
		for (Map.Entry<String, Short> e : termTFMap.entrySet()) {
			System.out.print("("+e.getKey()+", "+e.getValue()+")");
		}
		System.out.println();
	}
	
	public static void printTerms(String[] terms){
		System.out.print("Terms:");
		for (String t : terms) {
			System.out.print(" "+t);
		}
		System.out.println();
	}
	
	 private static String getContents(String aFile) {
	    //...checks on aFile are elided
	    StringBuilder contents = new StringBuilder();
	    
	    try {
	      //use buffering, reading one line at a time
	      //FileReader always assumes default encoding is OK!
	      BufferedReader input =  new BufferedReader(new FileReader(aFile));
	      try {
	        String line = null; //not declared within while loop
	        /*
	        * readLine is a bit quirky :
	        * it returns the content of a line MINUS the newline.
	        * it returns null only for the END of the stream.
	        * it returns an empty String if two newlines appear in a row.
	        */
	        while (( line = input.readLine()) != null){
	          contents.append(line);
	          contents.append(System.getProperty("line.separator"));
	        }
	      }
	      finally {
	        input.close();
	      }
	    }
	    catch (IOException ex){
	      ex.printStackTrace();
	    }
	    
	    return contents.toString();
	  }

	 
	 public static void main(String[] args){
		 //String text = DocumentProcessingUtils.getContents("c:/21190011.htm");
		 String text = DocumentProcessingUtils.getContents("c:/Copy of 13644224.htm");
		 System.out.println(text);
		 System.out.println("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*");
		 ivory.util.GalagoTokenizer mTokenizer = new ivory.util.GalagoTokenizer();
		 String[] terms = mTokenizer.processContent(text);
		 for(String t : terms) System.out.println(t);
	 }
	/*public static void main(String[] args){
		String docid;
		String content;
		Map<String, ArrayListOfInts> termPositionsMap;
		Map<String, Short> termTFMap;
		String[] terms;
		int docLength;
		
		docid = "d1";
		//content = "This is the first doc that is indexed and tokenized. This doc is the first";
		//content = " collection that contains no duplicate elements. More formally, sets contain no pair of elements e1 and e2 such that e1.equals(e2), and at most one null element. As implied by its name, this interface models the mathematical set abstraction."; 
		content = "one1 ,;`two2 three3 four4 five5 six6 seven7 eight8 nine9 ten10 one1 two2 three3 four4 five5 six6 seven7 eight8 nine9 ten10 one1 one1 one1 one1";
		
		PlainIndexable doc = new PlainIndexable(docid, content);
		Tokenizer tokenizer = new GalagoTokenizer();
		
		System.out.println("Doc :\""+doc.getContent()+"\"");
		terms = tokenizer.processContent(doc.getContent());
		
		DocumentProcessingUtils.printTerms(terms);
		termPositionsMap = DocumentProcessingUtils.getTermPositionsMap(doc, tokenizer);
		DocumentProcessingUtils.printTermPositionsMap(termPositionsMap);
		System.out.println("DocLength = "+DocumentProcessingUtils.getDocLengthFromPositionsMap(termPositionsMap));
		
		termTFMap = DocumentProcessingUtils.getTermTFMap(doc, tokenizer);
		DocumentProcessingUtils.printTermTFMap(termTFMap);
		System.out.println("DocLength = "+DocumentProcessingUtils.getDocLengthFromTFMap(termTFMap));
				
		
		//ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		//DataOutputStream dataOut = new DataOutputStream(bytesOut);
		
	}*/

	public static Map<String, ArrayListOfInts> getTermPositionsMap(String text, Tokenizer mTokenizer){
		// for storing the positions
		Map<String, ArrayListOfInts> positions = new HashMap<String, ArrayListOfInts>();
		
		String[] terms = mTokenizer.processContent(text);
	
		// the tokenizer may return
		// terms with zero length (empty terms), and the tf may exceed the
		// capacity of a short (in which case we need to handle separately).
		// The doc length and contribution to term count is computed as the
		// sum of all tfs of indexed terms a bit later.
	
		for (int i = 0; i < terms.length; i++) {
			String term = terms[i];
	
			// guard against bad tokenization
			if (term.length() == 0)
				continue;
	
			if (term.length() >= Byte.MAX_VALUE)
				continue;
	
			// remember, token position is numbered started from one...
			if (positions.containsKey(term)) {
				positions.get(term).add(i + 1);
			} else {
				ArrayListOfInts l = new ArrayListOfInts();
				l.add(i + 1);
				positions.put(term, l);
			}
		}
	
		Iterator<Map.Entry<String, ArrayListOfInts>> it = positions.entrySet().iterator();
		Map.Entry<String, ArrayListOfInts> e;
		ArrayListOfInts positionsList;
		while(it.hasNext()){
			e = it.next();
			positionsList = e.getValue(); // positions.get(e.getKey());
			
			// we're storing tfs as shorts, so check for overflow...
			if (positionsList.size() >= TF_CUT) {
				// There are a few ways to handle this... If we're getting
				// such a high tf, then it most likely means that this is a
				// junk doc. The current implementation simply skips this
				// posting...
				sLogger.warn("Error: tf of " + e.getValue()
						+ " will overflow max short value. term="
						+ e.getKey());
				//continue;
				it.remove();
				
			}
			positionsList.trimToSize();
		}
		return positions;
	}

}
