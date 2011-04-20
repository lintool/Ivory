package ivory.util;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ivory.data.PrefixEncodedGlobalStats;
import ivory.data.TermDocVector;
import ivory.data.TermDocVectorReader;
import ivory.pwsim.score.Bm25;
import ivory.pwsim.score.ScoringModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.clip.mt.Vocab;
import edu.umd.clip.mt.VocabularyWritable;
import edu.umd.clip.mt.alignment.IndexedFloatArray;
import edu.umd.clip.mt.ttables.TTable_monolithic_IFAs;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatString;
import edu.umd.cloud9.util.map.MapKF.Entry;

/**
 * Algorithms used in our CLIR approach to convert source language doc vectors into target language. See paper for details.
 * 
 * @author ferhanture
 *
 */
public abstract class CLIRUtils extends Configured {
	private static final Logger logger = Logger.getLogger(CLIRUtils.class);
	private static final int NUM_TRANS = 15;
	private static final float PROB_THRESHOLD = 0.9f;
	
	public static HMapIFW readTransDfTable(Path path, FileSystem localFs) {
		HMapIFW transDfTable = new HMapIFW();
		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(localFs, path, new Configuration());

			IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
			FloatWritable value = (FloatWritable) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				transDfTable.put(key.get(), value.get());

				key = (IntWritable) reader.getKeyClass().newInstance();
				value = (FloatWritable) reader.getValueClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			throw new RuntimeException("Exception reading file trans-df table file");
		}
		return transDfTable;		
	}

	public static double cosine(HMapSFW a, HMapSFW b) {
		double sum = 0, magA = 0, magB = 0;
		for(edu.umd.cloud9.util.map.MapKF.Entry<String> e : a.entrySet()){
			float value = e.getValue();
			magA += (value * value);
			if(b.containsKey(e.getKey())){
				sum+= value*b.get(e.getKey());
			}
		}
		for(edu.umd.cloud9.util.map.MapKF.Entry<String> e : b.entrySet()){
			float value = e.getValue();
			magB += (value * value);
		}
		return sum/(Math.sqrt(magA) * Math.sqrt(magB));

	}
	
	public static double cosineNormalized(HMapSFW a, HMapSFW b) {
		double sum = 0;
		for(edu.umd.cloud9.util.map.MapKF.Entry<String> e : a.entrySet()){
			float value = e.getValue();
			if(b.containsKey(e.getKey())){
				sum+= value*b.get(e.getKey());
			}
		}
		return sum;
	}

	public static HMapIFW translateDFTable(Vocab eVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2f_probs, PrefixEncodedGlobalStats globalStatsMap){
		HMapIFW transDfTable = new HMapIFW();
		for(int e=1;e<eVocabSrc.size();e++){
			int[] fS = e2f_probs.get(e).getTranslations(0.0f);
			float df=0;
			for(int f : fS){
				float probEF = e2f_probs.get(e, f);
				String fTerm = fVocabTrg.get(f);
				float df_f = globalStatsMap.getDF(fTerm);

				df+=(probEF*df_f);
			}
			transDfTable.put(e, df);
		}
		return transDfTable;
	}

	public static HMapIFW translateDFTable(Vocab eVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs e2f_probs, HMapSIW dfs){
		HMapIFW transDfTable = new HMapIFW();
		for(int e=1;e<eVocabSrc.size();e++){
			int[] fS = e2f_probs.get(e).getTranslations(0.0f);
			float df=0;
			for(int f : fS){
				float probEF = e2f_probs.get(e, f);
				String fTerm = fVocabTrg.get(f);
				if(!dfs.containsKey(fTerm)){	//only if word is in the collection, can it contribute to the df values.
					continue;
				}			
				float df_f = dfs.get(fTerm);
				df+=(probEF*df_f);
			}
			transDfTable.put(e, df);
		}
		return transDfTable;
	}

	public static HMapIFW updateTFsByTerm(String fTerm, int tf, HMapIFW tfS, Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs en2DeProbs, TTable_monolithic_IFAs f2e_Probs, Logger sLogger){
//		sLogger.setLevel(Level.WARN);
		int f = fVocabSrc.get(fTerm);
		if(f <= 0){
			sLogger.warn(f+","+fTerm+" word not in aligner's vocab (foreign side of f2e)");
			return tfS;
		}
		
		int[] eS = f2e_Probs.get(f).getTranslations(0.0f);
	
		int f2 = fVocabTrg.get(fTerm);		//different ids between two german vocabs
		if(f2 <= 0){
			sLogger.warn(fTerm+" word not in aligner's vocab (foreign side of e2f)");
			return tfS;
		}
		//tf(e) = sum_f{tf(f)*prob(f|e)}
		for(int e : eS){
			float probEF;
			String eTerm = eVocabTrg.get(e);
			int e2 = eVocabSrc.get(eTerm);		//convert between two english vocabs (different ids)
			if(e2 <= 0){
				sLogger.warn(eTerm+" word not in aligner's vocab (english side of e2f)");
				continue;
			}
			probEF = en2DeProbs.get(e2, f2);

			if(probEF > 0){
//				sLogger.debug(eVocabSrc.get(e2)+" ==> "+probEF);

				if(tfS.containsKey(e2)){
					tfS.put(e2, tfS.get(e2)+tf*probEF);
				}else{
					tfS.put(e2, tf*probEF);
				}
			}
		}
		return tfS;
	}
	
	public static int translateTFs(TermDocVector doc, HMapIFW tfS, Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs en2DeProbs, TTable_monolithic_IFAs f2e_Probs, Logger sLogger) throws IOException{
		if(sLogger == null){
			sLogger = logger;
		}
		//translate doc vector		
		TermDocVectorReader reader = doc.getDocVectorReader();
		int docLen = 0;
		while (reader.hasMoreTerms()) {
			String fTerm = reader.nextTerm();
			int tf = reader.getTf();
			docLen+=tf;

			int f = fVocabSrc.get(fTerm);
			if(f <= 0){
				sLogger.warn(f+","+fTerm+" word not in aligner's vocab (foreign side of f2e)");
				continue;
			}
			int[] eS = f2e_Probs.get(f).getTranslations(0.0f);

			int f2 = fVocabTrg.get(fTerm);		//different ids between two german vocabs
			if(f2 <= 0){
				sLogger.warn(fTerm+" word not in aligner's vocab (foreign side of e2f)");
				continue;
			}
			//tf(e) = sum_f{tf(f)*prob(f|e)}
			for(int e : eS){
				float probEF;
				String eTerm = eVocabTrg.get(e);
				int e2 = eVocabSrc.get(eTerm);		//convert between two english vocabs (different ids)
				if(e2 <= 0){
					sLogger.debug(eTerm+" word not in aligner's final vocab");
					continue;
				}
				probEF = en2DeProbs.get(e2, f2);
				if(probEF > 0){
					sLogger.debug(eVocabSrc.get(e2)+" ==> "+probEF);
					if(tfS.containsKey(e2)){
						tfS.put(e2, tfS.get(e2)+tf*probEF);
					}else{
						tfS.put(e2, tf*probEF);
					}
				}
			}
		}
		
		return docLen;
	}

	public static int translateTFs(HMapSIW doc, HMapIFW tfS, Vocab eVocabSrc, Vocab eVocabTrg, Vocab fVocabSrc, Vocab fVocabTrg, TTable_monolithic_IFAs en2DeProbs, TTable_monolithic_IFAs f2e_Probs, Logger sLogger) throws IOException{
		if(sLogger == null){
			sLogger = logger;
		}
		//translate doc vector		
		int docLen = 0;
		for(edu.umd.cloud9.util.map.MapKI.Entry<String> item : doc.entrySet()){
			String fTerm = item.getKey();
			int tf = item.getValue();
			docLen+=tf;

			int f = fVocabSrc.get(fTerm);
			if(f <= 0){
				sLogger.warn(f+","+fTerm+" word not in aligner's vocab (foreign side of f2e)");
				continue;
			}
			int[] eS = f2e_Probs.get(f).getTranslations(0.0f);

			int f2 = fVocabTrg.get(fTerm);		//different ids between two german vocabs
			if(f2 <= 0){
				sLogger.warn(fTerm+" word not in aligner's vocab (foreign side of e2f)");
				continue;
			}
			//tf(e) = sum_f{tf(f)*prob(f|e)}
			for(int e : eS){
				float probEF;
				String eTerm = eVocabTrg.get(e);
				int e2 = eVocabSrc.get(eTerm);		//convert between two english vocabs (different ids)
				if(e2 <= 0){
					sLogger.debug(eTerm+" word not in aligner's final vocab");
					continue;
				}
				probEF = en2DeProbs.get(e2, f2);
				if(probEF > 0){
					sLogger.debug(eVocabSrc.get(e2)+" ==> "+probEF);
					if(tfS.containsKey(e2)){
						tfS.put(e2, tfS.get(e2)+tf*probEF);
					}else{
						tfS.put(e2, tf*probEF);
					}
				}
			}
		}
		
		return docLen;
	}

	public static HMapSFW createTranslatedVector(int docLen, HMapIFW tfS, Vocab eVocabSrc, ScoringModel mModel, HMapIFW transDfTable, boolean isNormalize, Logger sLogger) {
		HMapSFW v = new HMapSFW();
		float normalization=0;
		for(int e : tfS.keySet()){
			//check if eng term is in collection's vocab
			String eTerm = eVocabSrc.get(e);
			float tf = tfS.get(e);
			float df = transDfTable.get(e);
			float score = ((Bm25)mModel).computeDocumentWeight(tf, df, docLen);
			if(score>0){
				v.put(eTerm, score);
				if(isNormalize){
					normalization+=Math.pow(score, 2);
				}		
			}
			sLogger.debug(eTerm+" "+tf+" "+df+" "+score);
		}

		/*length-normalize doc vectors*/
		if(isNormalize){
			normalization = (float) Math.sqrt(normalization);
			for(Entry<String> e : v.entrySet()){
				v.put(e.getKey(), e.getValue()/normalization);
			}
		}
		return v;
	}
	
	/**
	 * Copied from HadoopAligner package.
	 * 
	 * @param path to vocabulary file
	 * @param file system object
	 * @return Vocab object
	 * @throws IOException
	 */
	public static Vocab loadVocab(Path path, FileSystem fileSys) throws IOException {
		DataInput in = new DataInputStream(new BufferedInputStream(fileSys.open(path)));
		VocabularyWritable at = new VocabularyWritable();
		at.readFields(in);

		return at;
	}
	
	/**
	 * Copied from HadoopAligner package.
	 * 
	 * @param path to vocabulary file
	 * @param job/config object
	 * @return Vocab object
	 * @throws IOException
	 */
	static public Vocab loadVocab(Path path, Configuration job) throws IOException {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(job);
		FileSystem fileSys = FileSystem.get(conf);

		DataInput in = new DataInputStream(new BufferedInputStream(fileSys.open(path)));
		VocabularyWritable at = new VocabularyWritable();
		at.readFields(in);

		return at;
	}
	
	static public void extractTTableFromGIZA(String filename, String srcVocabFile, String trgVocabFile, String probsFile, FileSystem fs) throws IOException{
		TTable_monolithic_IFAs table = new TTable_monolithic_IFAs();
		VocabularyWritable trgVocab = new VocabularyWritable(), srcVocab = new VocabularyWritable();
		File file = new File(filename);
		FileInputStream fis = null;
		BufferedReader bis = null;
		int cnt = 0;

		try {
			fis = new FileInputStream(file);
			bis = new BufferedReader(new InputStreamReader(fis,"UTF-8"));

			String srcTerm = null, trgTerm = null, prev = null;
			int curIndex = -1;
			TreeSet<PairOfFloatString> topTrans = new TreeSet<PairOfFloatString>();
			String line = "";
			boolean earlyTerminate = false, skipTerm = false;
			float sumOfProbs = 0.0f, prob;

			while (true) {
				line = bis.readLine();
				if(line == null)	break;
				//logger.debug("Line:"+line);
				String[] parts = line.split(" ");
				if(parts.length!=3){
					throw new RuntimeException("Unknown format: "+line);
				}
				cnt++;
				trgTerm = parts[0];
				srcTerm = parts[1];
				prob = Float.parseFloat(parts[2]);
				if(prev==null || !srcTerm.equals(prev)){
					//store previous top translations to ttable
					if(topTrans.size()>0){
						int[] indices = new int[topTrans.size()];
						float[] probs = new float[topTrans.size()];
						int i=0;
						while(!topTrans.isEmpty()){
							PairOfFloatString e = topTrans.pollLast();
							String term = e.getRightElement();
							float pr = e.getLeftElement();
							int engIndex = trgVocab.addOrGet(term);
							indices[i]=engIndex;
							probs[i]=pr;
							i++;
						}
						table.set(curIndex, new IndexedFloatArray(indices, probs, false));
						
						//logger.debug(table.get(curIndex));
					}

					//initialize this term
					prev = srcTerm;
					int prevIndex = curIndex;
					curIndex = srcVocab.addOrGet(srcTerm);
					if(curIndex < prevIndex){
						//we've seen this foreign term before. should not happen. probably due to read error.
						skipTerm = true;
						continue;
					}
//					logger.debug("Processing: "+srcTerm+" with index: "+curIndex);
					earlyTerminate = false;		//reset status
					sumOfProbs = 0.0f;

					topTrans.clear();
					topTrans.add(new PairOfFloatString(prob, trgTerm));

					//logger.debug("Added: "+trgTerm+" with prob: "+prob);
					sumOfProbs += prob;
				}else if(!earlyTerminate && !skipTerm){	//continue adding translation term,prob pairs (except if early termination is ON)
					topTrans.add(new PairOfFloatString(prob, trgTerm));

					//logger.debug("Added: "+trgTerm+" with prob: "+prob);

					// keep top NUM_TRANS translations
					if(topTrans.size()>NUM_TRANS){
						topTrans.pollFirst();
					}
					sumOfProbs += prob;
				}else{
					//logger.debug("Skipped");
				}
				if(sumOfProbs > PROB_THRESHOLD){
					earlyTerminate = true;
					//logger.debug("Sum of probs > 0.9, early termination.");
				}
			}

			// dispose all the resources after using them.
			fis.close();
			bis.close();
		}catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.debug("Vocabulary English: "+trgVocab.size()+" elements");
		logger.debug("Vocabulary German: "+srcVocab.size()+" elements");

		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fs.create(new Path(trgVocabFile))));
		((VocabularyWritable) trgVocab).write(dos);
		dos.close();
		DataOutputStream dos2 = new DataOutputStream(new BufferedOutputStream(fs.create(new Path(srcVocabFile))));
		((VocabularyWritable) srcVocab).write(dos2);
		dos2.close();
		DataOutputStream dos3 = new DataOutputStream(new BufferedOutputStream(fs.create(new Path(probsFile))));
		table.write(dos3);
		dos3.close();
	}
	
	static public void extractTTableFromBerkeleyAligner(String inputFile, String srcVocabFile, String trgVocabFile, String probsFile, FileSystem fs) throws IOException{
		TTable_monolithic_IFAs table = new TTable_monolithic_IFAs();
		VocabularyWritable trgVocab = new VocabularyWritable(), srcVocab = new VocabularyWritable();
		File file = new File(inputFile);
		FileInputStream fis = null;
		BufferedReader bis = null;
		int cntLongTail = 0, cntShortTail = 0, sumShortTail = 0;

		try {
			fis = new FileInputStream(file);
			bis = new BufferedReader(new InputStreamReader(fis,"UTF-8"));

			String cur = null;
			boolean earlyTerminate = false;
			String line = "";

			while (true) {
				if(!earlyTerminate){
					line = bis.readLine();
					if(line ==null)
						break;
				}
				earlyTerminate = false;
				//				//logger.debug("Line:"+line);

				Pattern p = Pattern.compile("(.+)\\tentropy .+nTrans"); 
				Matcher m = p.matcher(line);
				if(m.find()){
					cur = m.group(1);

					int gerIndex = srcVocab.addOrGet(cur);	
					//					//logger.debug("Found: "+cur+" with index: "+gerIndex);

					int[] indices = new int[NUM_TRANS];
					float[] probs = new float[NUM_TRANS];

					float sumprob = 0.0f;
					for(int i=0;i<NUM_TRANS;i++){
						if((line=bis.readLine())!=null){
							//							line = dis.readLine();
							Pattern p2 = Pattern.compile("\\s*(\\S+): (.+)");
							Matcher m2 = p2.matcher(line);
							if(!m2.find()){
								m = p.matcher(line);
								if(m.find()){
									//									//logger.debug("Early terminate");
									earlyTerminate = true;
									i = NUM_TRANS;
									break;
								}
								//								//logger.debug("FFFF"+line);
							}else{
								String term = m2.group(1);
								float prob = Float.parseFloat(m2.group(2));
								int engIndex = trgVocab.addOrGet(term);
								//logger.debug("Added: "+term+" with index: "+engIndex+" and prob:"+prob);
								indices[i]=engIndex;
								probs[i]=prob;
								sumprob+=prob;
							}
						}
						if(sumprob > PROB_THRESHOLD){
							cntShortTail++;
							sumShortTail += i;
							break;
						}
					}
					if(sumprob < PROB_THRESHOLD){
						cntLongTail++;
						//logger.debug("Found: "+cur+" with index: "+gerIndex);
					}

					table.set(gerIndex, new IndexedFloatArray(indices, probs, false));
				}
			}

			// dispose all the resources after using them.
			fis.close();
			bis.close();
			//			dis.close();
		}catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//logger.debug("Vocabulary Target: "+trgVocab.size()+" elements");
		//logger.debug("Vocabulary Source: "+srcVocab.size()+" elements");
		//logger.debug("# source terms with > 0.9 probability covered: "+cntShortTail+" and average translations per term: "+(sumShortTail/(cntShortTail+1.0f)));
		//logger.debug("# source terms with <= 0.9 probability covered: "+cntLongTail+" (each has 15 translations)");


		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream
				(fs.create(new Path(trgVocabFile))));
		((VocabularyWritable) trgVocab).write(dos);
		dos.close();
		DataOutputStream dos2 = new DataOutputStream(new BufferedOutputStream
				(fs.create(new Path(srcVocabFile))));
		((VocabularyWritable) srcVocab).write(dos2);
		dos2.close();
		DataOutputStream dos3 = new DataOutputStream(new BufferedOutputStream
				(fs.create(new Path(probsFile))));
		table.write(dos3);
		dos3.close();
	}

	public static void main(String[] args){
		logger.setLevel(Level.DEBUG);
		try {
			extractTTableFromGIZA("/Users/ferhanture/edu/research_archive/data/de-en/giza_de-en.align", "/Users/ferhanture/edu/research_archive/data/de-en/eu-nc-wmt08/giza-vocab_de-en.de", "/Users/ferhanture/edu/research_archive/data/de-en/eu-nc-wmt08/giza-vocab_de-en.en", "/Users/ferhanture/edu/research_archive/data/de-en/eu-nc-wmt08/giza_ttable.de-en", FileSystem.getLocal(new Configuration()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
