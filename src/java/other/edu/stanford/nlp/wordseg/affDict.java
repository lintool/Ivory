package edu.stanford.nlp.wordseg;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * affixation information
 * @author Huihsin Tseng
 * @author Pichuan Chang
 */


@SuppressWarnings("unused")
public class affDict {
	private String sighanCorporaDict = "/u/nlp/data/chinese-segmenter/";
	private String affixFilename;


	//public Set ctbIns, asbcIns, hkIns, pkIns, msrIns;
	public Set<String> ins;

	public affDict(String affixFilename)  {
		ins=readDict(affixFilename);
	}

	/**
	 * @author ferhanture
	 * @param stream
	 **/
	public affDict(FSDataInputStream stream)  {
		ins=readDict(stream); 
	}

	Set<String> getInDict() {return ins;}



	private Set<String> readDict(String filename)  {
		Set<String> a = new HashSet<String>();

		//System.err.println("XM:::readDict(filename: " + filename + ")");
		try {
			BufferedReader aDetectorReader;
			/*
      if(filename.endsWith("in.as") ||filename.endsWith("in.city") ){
      	aDetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "Big5_HKSCS"));
      }else{ aDetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "GB18030"));
      }
			 */
			aDetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));

			String aDetectorLine;

			//System.err.println("DEBUG: in affDict readDict");
			while ((aDetectorLine = aDetectorReader.readLine()) != null) {
				//System.err.println("DEBUG: affDict: "+filename+" "+aDetectorLine);
				a.add(aDetectorLine);
			}
		} catch (FileNotFoundException e) {
			System.err.println("affDict: File not found");
			System.err.println("filename: " + filename);
			System.exit(-1);
		} catch (IOException e) {
			System.exit(-1);
		}
		return a;
	}

	/**
	 * @author ferhanture
	 */
	private Set<String> readDict(FSDataInputStream stream)  {
		Set<String> a = new HashSet<String>();

		//System.err.println("XM:::readDict(filename: " + filename + ")");
		try {
			//			BufferedReader aDetectorReader;
			/*
      if(filename.endsWith("in.as") ||filename.endsWith("in.city") ){
      	aDetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "Big5_HKSCS"));
      }else{ aDetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "GB18030"));
      }
			 */
			//			aDetectorReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));

			String aDetectorLine;

			//System.err.println("DEBUG: in affDict readDict");
			while ((aDetectorLine = stream.readLine()) != null) {
				//System.err.println("DEBUG: affDict: "+filename+" "+aDetectorLine);
				a.add(aDetectorLine);
			}
		} catch (FileNotFoundException e) {
			System.err.println("affDict: Stream not found");
			System.exit(-1);
		} catch (IOException e) {
			System.exit(-1);
		}
		return a;
	}

	public String getInDict(String a1) {
		if (getInDict().contains(a1))
			return "1";
		return "0";
	}
}//end of class
