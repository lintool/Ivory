package edu.stanford.nlp.wordseg;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.FSDataInputStream;


/**
 * Check tag of each character from 5 different corpora. (4 official training corpora of Sighan bakeoff 2005, plus CTB)
 * These tags are not external knowledge. They are learned from the training corpora.
 * @author Huihsin Tseng
 * @author Pichuan Chang
 */


public class CorpusChar {
	@SuppressWarnings("unused")
	private String sighanCorporaDict = "/u/nlp/data/chinese-segmenter/";
	/*
  private String ctbFilename;
  private String asbcFilename;
  private String pkuFilename;
  private String msrFilename;
  private String hkFilename;
  private String charlistFilename;
	 */

	/*
  public HashMap <String, Set <String>> ctbchar;
  public HashMap <String, Set <String>> asbcchar;
  public HashMap <String, Set <String>> pkuchar;
  public HashMap <String, Set <String>> msrchar;
  public HashMap <String, Set <String>> hkchar;
	 */
	private HashMap <String, Set <String>> charMap;

	public CorpusChar(String charlistFilename)  {
		/*
    this.sighanCorporaDict = sighanCorporaDict;
    ctbFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.ctb.list";
    asbcFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.as.list";
    pkuFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.pku.list";
    msrFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.msr.list";
    hkFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.city.list";
    ctbchar=readDict(ctbFilename);
    asbcchar=readDict(asbcFilename);
    pkuchar=readDict(pkuFilename);
    msrchar=readDict(msrFilename);
    hkchar=readDict(hkFilename);
		 */
		charMap=readDict(charlistFilename); 
	}

	/**
	 * @author ferhanture
	 * @param stream
	 */
	public CorpusChar(FSDataInputStream stream)  {
		charMap=readDict(stream); 
	}

	@SuppressWarnings("unused")
	private CorpusChar()  {
		/*
    ctbFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.ctb.list";
    asbcFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.as.list";
    pkuFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.pku.list";
    msrFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.msr.list";
    hkFilename = sighanCorporaDict+"/Sighan2005/dict/pos_close/char.city.list";

    ctbchar=readDict(ctbFilename);
    asbcchar=readDict(asbcFilename);
    pkuchar=readDict(pkuFilename);
    msrchar=readDict(msrFilename);
    hkchar=readDict(hkFilename);
		 */
	}

	/*
  HashMap  getctb(){
    return ctbchar;
  }

  HashMap getasbc(){
    return asbcchar;
  }

  HashMap getpku(){
    return pkuchar;
  }

  HashMap gethk(){
    return hkchar;
  }
  HashMap getmsr(){
    return msrchar;
  }*/
	HashMap<String, Set<String>> getCharMap() {
		return charMap;
	}


	private HashMap <String, Set <String>> char_dict;

	private HashMap<String, Set<String>> readDict(String filename)  {

		try {
			BufferedReader DetectorReader;
			/*
      if( filename.endsWith("char.as.list") ||  filename.endsWith("char.city.list")  ){
  	DetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "Big5_HKSCS"));
      }else{
 	DetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "GB18030"));
      }
			 */
			DetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));
			String DetectorLine;

			char_dict = new HashMap<String, Set <String>>();
			//System.err.println("DEBUG: in CorpusChar readDict");
			while ((DetectorLine = DetectorReader.readLine()) != null) {

				String[] fields = DetectorLine.split("	");
				String tag=fields[0];

				Set<String> chars=char_dict.get(tag);

				if(chars==null){
					chars = new HashSet<String>();
					char_dict.put(tag,chars);
				} 
				//System.err.println("DEBUG: CorpusChar: "+filename+" "+fields[1]);
				chars.add(fields[1]);


			}
		} catch (FileNotFoundException e) {
			System.err.println("*NOT* *NOT* *NOT*  no file");
			System.err.println("filename: " + filename);
			System.exit(-1);
		} catch (IOException e) {
			System.err.println("*NOT* *NOT* *NOT*");
			System.exit(-1);
		}
		return char_dict;
	}

	private HashMap<String, Set<String>> readDict(FSDataInputStream stream)  {

		try {
			BufferedReader DetectorReader;
			/*
	      if( filename.endsWith("char.as.list") ||  filename.endsWith("char.city.list")  ){
	  	DetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "Big5_HKSCS"));
	      }else{
	 	DetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "GB18030"));
	      }
			 */
			//	      DetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));
			String DetectorLine;

			char_dict = new HashMap<String, Set <String>>();
			//System.err.println("DEBUG: in CorpusChar readDict");
			while ((DetectorLine = stream.readLine()) != null) {

				String[] fields = DetectorLine.split("	");
				String tag=fields[0];

				Set<String> chars=char_dict.get(tag);

				if(chars==null){
					chars = new HashSet<String>();
					char_dict.put(tag,chars);
				} 
				//System.err.println("DEBUG: CorpusChar: "+filename+" "+fields[1]);
				chars.add(fields[1]);


			}
		} catch (FileNotFoundException e) {
			System.err.println("*NOT* *NOT* *NOT*  no stream");
			System.exit(-1);
		} catch (IOException e) {
			System.err.println("*NOT* *NOT* *NOT*");
			System.exit(-1);
		}
		return char_dict;
	}

	public String getTag(String a1, String a2) {
		HashMap<String, Set<String>> h1=getCharMap();
		Set<String> h2=h1.get(a1);
		if (h2 == null) return "0";
		if (h2.contains(a2)) 
			return "1";
		return "0"; 
	}
	/*
  public String getCtbTag(String a1, String a2) {
    HashMap h1=dict.getctb();
    Set h2=(Set)h1.get(a1);
    if (h2 == null) return "0";
    if (h2.contains(a2)) 
      return "1";
    return "0"; 
  }

  public String getAsbcTag(String a1, String a2) {
    HashMap h1=dict.getasbc();
    Set h2=(Set)h1.get(a1);
    if (h2 == null) return "0";
    if (h2.contains(a2)) 
      return "1";
    return "0"; 
  }

  public String getPkuTag(String a1, String a2) {
    HashMap h1=dict.getpku();
    Set h2=(Set)h1.get(a1);
    if (h2 == null) return "0";
    if (h2.contains(a2)) 
      return "1";
    return "0"; 
  }

  public String getHkTag(String a1, String a2) {
    HashMap h1=dict.gethk();
    Set h2=(Set)h1.get(a1);
    if (h2 == null) return "0";
    if (h2.contains(a2)) 
      return "1";
    return "0"; 
  }


  public String getMsrTag(String a1, String a2) {
    HashMap h1=dict.getmsr();
    Set h2=(Set)h1.get(a1);
    if (h2 == null) return "0";
    if (h2.contains(a2)) 
      return "1";
    return "0"; 
    }*/

}//end of class
