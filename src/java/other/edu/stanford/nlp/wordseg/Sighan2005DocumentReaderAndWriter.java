package edu.stanford.nlp.wordseg;

import static edu.stanford.nlp.trees.international.pennchinese.ChineseUtils.WHITE;
import static edu.stanford.nlp.trees.international.pennchinese.ChineseUtils.WHITEPLUS;

import java.io.File;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;

import edu.stanford.nlp.io.EncodingPrintWriter;
import edu.stanford.nlp.ling.CoreAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CharAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.D2_LBeginAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.D2_LEndAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.D2_LMiddleAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.LBeginAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.LEndAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.LMiddleAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.OriginalCharAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PositionAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.ShapeAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SpaceBeforeAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.UBlockAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.UTypeAnnotation;
import edu.stanford.nlp.objectbank.IteratorFromReaderFactory;
import edu.stanford.nlp.objectbank.LineIterator;
import edu.stanford.nlp.objectbank.ObjectBank;
import edu.stanford.nlp.process.ChineseDocumentToSentenceProcessor;
import edu.stanford.nlp.util.Characters;
import edu.stanford.nlp.util.Function;
import edu.stanford.nlp.sequences.DocumentReaderAndWriter;
import edu.stanford.nlp.sequences.LatticeWriter;
import edu.stanford.nlp.sequences.SeqClassifierFlags;
import edu.stanford.nlp.trees.international.pennchinese.ChineseUtils;
import edu.stanford.nlp.util.StringUtils;
import edu.stanford.nlp.util.MutableInteger;
import edu.stanford.nlp.fsm.DFSA;
import edu.stanford.nlp.fsm.DFSAState;
import edu.stanford.nlp.fsm.DFSATransition;

/**
 * DocumentReader for Chinese segmentation task. (Sighan bakeoff 2005)
 * Reads in characters and labels them as 1 or 0 (word START or NONSTART).
 * <p>
 * Note: maybe this can do less interning, since some is done in
 * ObjectBankWrapper, but this also calls trim() as it works....
 *
 * @author Pi-Chuan Chang
 * @author Michel Galley (Viterbi seearch graph printing)
 */
public class Sighan2005DocumentReaderAndWriter implements DocumentReaderAndWriter<CoreLabel>, LatticeWriter<CoreLabel> {

	private static final boolean DEBUG = false;
	private static final boolean DEBUG_MORE = false;

	// year, month, day chars.  Sometime try adding \u53f7 and see if it helps...
	private static final Pattern dateChars = Pattern.compile("[\u5E74\u6708\u65E5]");
	// year, month, day chars.  Adding \u53F7 and seeing if it helps...
	private static final Pattern dateCharsPlus = Pattern.compile("[\u5E74\u6708\u65E5\u53f7]");
	// number chars (Chinese and Western).
	// You get U+25CB circle masquerading as zero in mt data - or even in Sighan 2003 ctb
	// add U+25EF for good measure (larger geometric circle)
	private static final Pattern numberChars = Pattern.compile("[0-9\uff10-\uff19" +
			"\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4E5D\u5341" +
	"\u96F6\u3007\u767E\u5343\u4E07\u4ebf\u5169\u25cb\u25ef\u3021-\u3029\u3038-\u303A]");
	// A-Za-z, narrow and full width
	private static final Pattern letterChars = Pattern.compile("[A-Za-z\uFF21-\uFF3A\uFF41-\uFF5A]");
	private static final Pattern periodChars = Pattern.compile("[\ufe52\u2027\uff0e.\u70B9]");

	// two punctuation classes for Low and Ng style features.
	private final Pattern separatingPuncChars = Pattern.compile("[]!\"(),;:<=>?\\[\\\\`{|}~^\u3001-\u3003\u3008-\u3011\u3014-\u301F\u3030" +
			"\uff3d\uff01\uff02\uff08\uff09\uff0c\uff1b\uff1a\uff1c\uff1d\uff1e\uff1f" +
	"\uff3b\uff3c\uff40\uff5b\uff5c\uff5d\uff5e\uff3e]");
	private final Pattern ambiguousPuncChars = Pattern.compile("[-#$%&'*+/@_\uff0d\uff03\uff04\uff05\uff06\uff07\uff0a\uff0b\uff0f\uff20\uff3f]");
	private final Pattern midDotPattern = Pattern.compile(ChineseUtils.MID_DOT_REGEX_STR);

	private ChineseDocumentToSentenceProcessor cdtos;
	private ChineseDictionary cdict, cdict2;
	private SeqClassifierFlags flags;
	private IteratorFromReaderFactory<List<CoreLabel>> factory;

	public Iterator<List<CoreLabel>> getIterator(Reader r) {
		return factory.getIterator(r);
	}

	public void init(SeqClassifierFlags flags) {
		this.flags = flags;
		factory = LineIterator.getFactory(new CTBDocumentParser());
		if (DEBUG) EncodingPrintWriter.err.println("Sighan2005DocRandW: using normalization file " + flags.normalizationTable, "UTF-8");
		// pichuan : flags.normalizationTable is null --> i believe this is replaced by some java class??
		// (Thu Apr 24 11:10:42 2008)
		cdtos = new ChineseDocumentToSentenceProcessor(flags.normalizationTable);

		if (flags.dictionary != null) {
			String[] dicts = flags.dictionary.split(",");
			cdict = new ChineseDictionary(dicts, cdtos, flags.expandMidDot);
		}
		if (flags.serializedDictionary != null) {
			String dict = flags.serializedDictionary;
			cdict = new ChineseDictionary(dict, cdtos, flags.expandMidDot);
		}

		if (flags.dictionary2 != null) {
			String[] dicts2 = flags.dictionary2.split(",");
			cdict2 = new ChineseDictionary(dicts2, cdtos, flags.expandMidDot);
		}
	}

	/**
	 * @author ferhanture
	 * 
	 * @assume dictionary needs to be in single file and gzipped
	**/
	public void init(FSDataInputStream stream, SeqClassifierFlags flags) {
		this.flags = flags;
		factory = LineIterator.getFactory(new CTBDocumentParser());
		if (DEBUG) EncodingPrintWriter.err.println("Sighan2005DocRandW: using normalization file " + flags.normalizationTable, "UTF-8");
		// pichuan : flags.normalizationTable is null --> i believe this is replaced by some java class??
		// (Thu Apr 24 11:10:42 2008)
		cdtos = new ChineseDocumentToSentenceProcessor(flags.normalizationTable);

		if (flags.serializedDictionary != null) {
			cdict = new ChineseDictionary(stream, cdtos, flags.expandMidDot);
		}

	}

	public class CTBDocumentParser implements Function<String,List<CoreLabel>> {
		private String defaultMap = "char=0,answer=1";
		public String[] map = StringUtils.mapStringToArray(defaultMap);


		public List<CoreLabel> apply(String line) {
			if (line == null) {
				return null;
			}

			//Matcher tagMatcher = tagPattern.matcher(line);
			//line = tagMatcher.replaceAll("");
			line = line.trim();

			List<CoreLabel> lwi = new ArrayList<CoreLabel>();
			String origLine = line;
			if (DEBUG) EncodingPrintWriter.err.println("ORIG: " + line, "UTF-8");
			line = cdtos.normalization(origLine);
			if (DEBUG) EncodingPrintWriter.err.println("NORM: " + line, "UTF-8");
			int origIndex = 0;
			int position = 0;

			StringBuilder nonspaceLineSB = new StringBuilder();

			for (int i = 0, len = line.length(); i < len; i++) {
				char ch = line.charAt(i);
				CoreLabel wi = new CoreLabel();
				String wordString = Character.toString(ch);
				if ( ! Character.isWhitespace(ch) && ! Character.isISOControl(ch)) {
					wi.set(CharAnnotation.class, intern(wordString));
					nonspaceLineSB.append(wordString);

					while (Character.isWhitespace(origLine.charAt(origIndex)) || Character.isISOControl(origLine.charAt(origIndex))) {
						origIndex++;
					}

					wordString = Character.toString(origLine.charAt(origIndex));
					wi.set(OriginalCharAnnotation.class, intern(wordString));

					// put in a word shape
					if (flags.useShapeStrings) {
						wi.set(ShapeAnnotation.class, shapeOf(wordString));
					}
					if (flags.useUnicodeType || flags.useUnicodeType4gram || flags.useUnicodeType5gram) {
						wi.set(UTypeAnnotation.class, Character.getType(ch));
					}
					if (flags.useUnicodeBlock) {
						wi.set(UBlockAnnotation.class, Characters.unicodeBlockStringOf(ch));
					}

					origIndex++;

					if (i == 0) { // first character of a sentence (a line)
						wi.set(AnswerAnnotation.class, "1");
						wi.set(SpaceBeforeAnnotation.class, "1");
					} else if (Character.isWhitespace(line.charAt(i - 1)) || Character.isISOControl(line.charAt(i-1))) {
						wi.set(AnswerAnnotation.class, "1");
						wi.set(SpaceBeforeAnnotation.class, "1");
					} else {
						wi.set(AnswerAnnotation.class, "0");
						wi.set(SpaceBeforeAnnotation.class, "0");
					}
					wi.set(PositionAnnotation.class, intern(String.valueOf((position))));
					position++;
					if (DEBUG_MORE) EncodingPrintWriter.err.println(wi.toString(), "UTF-8");
					lwi.add(wi);
				}
			}
			if (flags.dictionary != null || flags.serializedDictionary != null) {
				String nonspaceLine = nonspaceLineSB.toString();
				addDictionaryFeatures(cdict, LBeginAnnotation.class, LMiddleAnnotation.class, LEndAnnotation.class, nonspaceLine, lwi);
			}

			if (flags.dictionary2 != null) {
				String nonspaceLine = nonspaceLineSB.toString();
				addDictionaryFeatures(cdict2, D2_LBeginAnnotation.class, D2_LMiddleAnnotation.class, D2_LEndAnnotation.class, nonspaceLine, lwi);
			}
			return lwi;
		}
	}

	/** Calculates a character shape for Chinese. */
	private String shapeOf(String input) {
		String shape;
		if (flags.augmentedDateChars && Sighan2005DocumentReaderAndWriter.dateCharsPlus.matcher(input).matches()) {
			shape = "D";
		} else if (Sighan2005DocumentReaderAndWriter.dateChars.matcher(input).matches()) {
			shape = "D";
		} else if (Sighan2005DocumentReaderAndWriter.numberChars.matcher(input).matches()) {
			shape = "N";
		} else if (Sighan2005DocumentReaderAndWriter.letterChars.matcher(input).matches()) {
			shape = "L";
		} else if (Sighan2005DocumentReaderAndWriter.periodChars.matcher(input).matches()) {
			shape = "P";
		} else if (separatingPuncChars.matcher(input).matches()) {
			shape = "S";
		} else if (ambiguousPuncChars.matcher(input).matches()) {
			shape = "A";
		} else if (flags.useMidDotShape && midDotPattern.matcher(input).matches()) {
			shape = "M";
		} else {
			shape = "C";
		}
		return shape;
	}


	private static void addDictionaryFeatures(ChineseDictionary dict, Class<? extends CoreAnnotation<String>> lbeginFieldName, Class<? extends CoreAnnotation<String>> lmiddleFieldName, Class<? extends CoreAnnotation<String>> lendFieldName, String nonspaceLine, List<CoreLabel> lwi) {
		int lwiSize = lwi.size();
		if (lwiSize != nonspaceLine.length()) { throw new RuntimeException(); }
		int[] lbegin = new int[lwiSize];
		int[] lmiddle = new int[lwiSize];
		int[] lend = new int[lwiSize];
		for (int i = 0; i < lwiSize; i++) {
			lbegin[i] = lmiddle[i] = lend[i] = 0;
		}
		for (int i = 0; i < lwiSize; i++) {
			for (int leng = ChineseDictionary.MAX_LEXICON_LENGTH; leng >= 1; leng--) {
				if (i+leng-1 < lwiSize) {
					if (dict.contains(nonspaceLine.substring(i, i+leng))) {
						// lbegin
						if (leng > lbegin[i]) {
							lbegin[i] = leng;
						}
						// lmid
						int last = i+leng-1;
						if (leng==ChineseDictionary.MAX_LEXICON_LENGTH) { last+=1; }
						for (int mid = i+1; mid < last; mid++) {
							if (leng > lmiddle[mid]) {
								lmiddle[mid] = leng;
							}
						}
						// lend
						if (leng<ChineseDictionary.MAX_LEXICON_LENGTH) {
							if (leng > lend[i+leng-1]) {
								lend[i+leng-1] = leng;
							}
						}
					}
				}
			}
		}
		for (int i = 0; i < lwiSize; i++) {
			StringBuilder sb = new StringBuilder();
			sb.append(lbegin[i]);
			if (lbegin[i]==ChineseDictionary.MAX_LEXICON_LENGTH) {
				sb.append("+");
			}
			lwi.get(i).set(lbeginFieldName, sb.toString());

			sb = new StringBuilder();
			sb.append(lmiddle[i]);
			if (lmiddle[i]==ChineseDictionary.MAX_LEXICON_LENGTH) {
				sb.append("+");
			}
			lwi.get(i).set(lmiddleFieldName, sb.toString());

			sb = new StringBuilder();
			sb.append(lend[i]);
			if (lend[i]==ChineseDictionary.MAX_LEXICON_LENGTH) {
				sb.append("+");
			}
			lwi.get(i).set(lendFieldName, sb.toString());

			//System.err.println(lwi.get(i));
		}
	}

	private static boolean isLetterASCII(char c) {
		return c <= 127 && Character.isLetter(c);
	}

	public void printAnswers(List<CoreLabel> doc, PrintWriter pw) {
		try {
			// Hey all: Some of the code that was previously here for
			// whitespace normalization was a bit hackish as well as
			// obviously broken for some test cases. So...I went ahead and
			// re-wrote it.
			//
			// Also, putting everything into 'testContent', is a bit wasteful
			// memory wise. But, it's on my near-term todo list to
			// code something thats a bit more memory efficient.
			//
			// Finally, if these changes ended up breaking anything
			// just e-mail me (cerd@colorado.edu), and I'll try to fix it
			// asap  -cer (6/14/2006)

			/* Sun Oct  7 19:55:09 2007
         I'm actually not using "testContent" anymore.
         I think it's broken because the whole test file has been read over and over again,
         tand the testContentIdx has been set to 0 every time, while "doc" is moving
         line by line!!!!
         -pichuan
			 */

			int testContentIdx=0;
			StringBuilder ans = new StringBuilder(); // the actual output we will return
			StringBuilder unmod_ans = new StringBuilder();  // this is the original output from the CoreLabel
			StringBuilder unmod_normed_ans = new StringBuilder();  // this is the original output from the CoreLabel
			CoreLabel wi = null;
			for (Iterator<CoreLabel> wordIter = doc.iterator(); wordIter.hasNext();
			testContentIdx++) {
				CoreLabel pwi = wi;
				wi = wordIter.next();
				//System.err.println(wi);
				boolean originalWhiteSpace = "1".equals(wi.get(SpaceBeforeAnnotation.class));

				//  if the CRF says "START" (segmented), and it's not the first word..
				if (wi.get(AnswerAnnotation.class).equals("1") && !("0".equals(String.valueOf(wi.get(PositionAnnotation.class))))) {
					// check if we need to preserve the "no space" between English
					// characters
					boolean seg = true; // since it's in the "1" condition.. default
					// is to seg
					if (flags.keepEnglishWhitespaces) {
						if (testContentIdx > 0) {
							char prevChar = pwi.get(OriginalCharAnnotation.class).charAt(0);
							char currChar = wi.get(OriginalCharAnnotation.class).charAt(0);
							if (isLetterASCII(prevChar) && isLetterASCII(currChar)) {
								// keep the "non space" before wi
								if (! originalWhiteSpace) {
									seg = false;
								}
							}
						}
					}

					// if there was space and keepAllWhitespaces is true, restore it no matter what
					if (flags.keepAllWhitespaces && originalWhiteSpace) {
						seg = true;
					}
					if (seg) {
						if (originalWhiteSpace) {
							ans.append('\u1924'); // a pretty Limbu character which is later changed to a space
						} else {
							ans.append(' ');
						}
					}
					unmod_ans.append(' ');
					unmod_normed_ans.append(' ');
				} else {
					boolean seg = false; // since it's in the "0" condition.. default
					// Changed after conversation with Huihsin.
					//
					// Decided that all words consisting of English/ASCII characters
					// should be separated from the surrounding Chinese characters. -cer
					/* Sun Oct  7 22:14:46 2007 (pichuan)
             the comment above was from DanC.
             I changed the code but I think I'm doing the same thing here.
					 */
					if (testContentIdx > 0) {
						char prevChar = pwi.get(OriginalCharAnnotation.class).charAt(0);
						char currChar = wi.get(OriginalCharAnnotation.class).charAt(0);
						if ((prevChar < (char)128) != (currChar < (char)128)) {
							if (ChineseUtils.isNumber(prevChar) && ChineseUtils.isNumber(currChar)) {
								// cdm: you would get here if you had an ASCII number next to a
								// Unihan range number.  Does that happen?  It presumably
								// shouldn't do any harm.... [cdm, oct 2007]
							} else {
								seg = true;
							}
						}
					}

					if (flags.keepEnglishWhitespaces) {
						if (testContentIdx > 0) {
							char prevChar = pwi.get(OriginalCharAnnotation.class).charAt(0);
							char currChar = wi.get(OriginalCharAnnotation.class).charAt(0);
							if (isLetterASCII(prevChar) && isLetterASCII(currChar) ||
									isLetterASCII(prevChar) &&  ChineseUtils.isNumber(currChar) ||
									ChineseUtils.isNumber(prevChar) && isLetterASCII(currChar)) {
								// keep the "space" before wi
								if ("1".equals(wi.get(SpaceBeforeAnnotation.class))) {
									seg = true;
								}
							}
						}
					}

					// if there was space and keepAllWhitespaces is true, restore it no matter what
					if (flags.keepAllWhitespaces) {
						if (!("0".equals(String.valueOf(wi.get(PositionAnnotation.class))))
								&& "1".equals(wi.get(SpaceBeforeAnnotation.class))) {
							seg = true;
						}
					}
					if (seg) {
						if (originalWhiteSpace) {
							ans.append('\u1924'); // a pretty Limbu character which is later changed to a space
						} else {
							ans.append(' ');
						}
					}
				}
				ans.append(wi.get(OriginalCharAnnotation.class)); // wi.get(OriginalCharAnnotation.class)); : word
				unmod_ans.append(wi.get(OriginalCharAnnotation.class));
				unmod_normed_ans.append(wi.get(CharAnnotation.class));
			}
			String ansStr = ans.toString();
			if (flags.sighanPostProcessing) {
				if ( ! flags.keepAllWhitespaces) {
					// remove the Limbu char now, so it can be deleted in postprocessing
					ansStr = ansStr.replaceAll("\u1924", " ");
				}
				ansStr = postProcessingAnswer(ansStr);
			}
			// definitely remove the Limbu char if it survived till now
			ansStr = ansStr.replaceAll("\u1924", " ");
			pw.print(ansStr);
			if (DEBUG) {
				EncodingPrintWriter.err.println("CLASSIFIER(normed): " + unmod_normed_ans, "UTF-8");
				EncodingPrintWriter.err.println("CLASSIFIER: " + unmod_ans, "UTF-8");
				EncodingPrintWriter.err.println("POSTPROCESSED: "+ans, "UTF-8");
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		pw.println();
	}



	/**
	 * post process the answer to be output
	 * these post processing are not dependent on original input
	 */
	private String postProcessingAnswer(String ans) {
		if (flags.useHk) {
			//System.err.println("Using HK post processing.");
			return postProcessingAnswerHK(ans);
		} else if (flags.useAs) {
			//System.err.println("Using AS post processing.");
			return postProcessingAnswerAS(ans);
		} else if (flags.usePk) {
			//System.err.println("Using PK post processing.");
			return postProcessingAnswerPK(ans,flags.keepAllWhitespaces);
		} else if (flags.useMsr) {
			//System.err.println("Using MSR post processing.");
			return postProcessingAnswerMSR(ans);
		} else {
			//System.err.println("Using CTB post processing.");
			return postProcessingAnswerCTB(ans,flags.keepAllWhitespaces);
		}
	}

	static Pattern[] puncsPat = null;
	static Character[] puncs = null;

	private static String separatePuncs(String ans) {
		/* make sure some punctuations will only appeared as one word (segmented from others). */
		/* These punctuations are derived directly from the training set. */
		if (puncs == null) {
			puncs = new Character[]{'\u3001', '\u3002', '\u3003', '\u3008', '\u3009', '\u300a', '\u300b',
					'\u300c', '\u300d', '\u300e', '\u300f', '\u3010', '\u3011', '\u3014',
			'\u3015'};
		}
		if (puncsPat == null) {
			//System.err.println("Compile Puncs");
			puncsPat = new Pattern[puncs.length];
			for(int i = 0; i < puncs.length; i++) {
				Character punc = puncs[i];
				puncsPat[i] = Pattern.compile(WHITE + punc + WHITE);
			}
		}
		for (int i = 0; i < puncsPat.length; i++) {
			Pattern p = puncsPat[i];
			Character punc = puncs[i];
			Matcher m = p.matcher(ans);
			ans = m.replaceAll(" "+punc+" ");
		}
		ans = ans.trim();
		return ans;
	}

	private static String separatePuncs(Character[] puncs_in, String ans) {
		/* make sure some punctuations will only appeared as one word (segmented from others). */
		/* These punctuations are derived directly from the training set. */
		if (puncs == null) { puncs = puncs_in; }
		if (puncsPat == null) {
			//System.err.println("Compile Puncs");
			puncsPat = new Pattern[puncs.length];
			for(int i = 0; i < puncs.length; i++) {
				Character punc = puncs[i];
				if (punc == '(' || punc == ')') { // escape
					puncsPat[i] = Pattern.compile(WHITE + "\\" + punc + WHITE);
				} else {
					puncsPat[i] = Pattern.compile(WHITE + punc + WHITE);
				}
			}
		}

		for (int i = 0; i < puncsPat.length; i++) {
			Pattern p = puncsPat[i];
			Character punc = puncs[i];
			Matcher m = p.matcher(ans);
			ans = m.replaceAll(" "+punc+" ");
		}
		ans = ans.trim();
		return ans;
	}

	/** The one extant use of this method is to connect a U+30FB (Katakana midDot
	 *  with preceding and following non-space characters (in CTB
	 *  postprocessing). I would hypothesize that if mid dot chars were correctly
	 *  recognized in shape contexts, then this would be unnecessary [cdm 2007].
	 *  Also, note that IBM GALE normalization seems to produce U+30FB and not
	 *  U+00B7.
	 *
	 *  @param punc character to be joined to surrounding chars
	 *  @param ans Input string which may or may not contain punc
	 *  @return String with spaces removed between any instance of punc and
	 *      surrounding chars.
	 */
	private static String gluePunc(Character punc, String ans) {
		Pattern p = Pattern.compile(WHITE + punc);
		Matcher m = p.matcher(ans);
		ans = m.replaceAll(String.valueOf(punc));
		p = Pattern.compile(punc + WHITE);
		m = p.matcher(ans);
		ans = m.replaceAll(String.valueOf(punc));
		ans = ans.trim();
		return ans;
	}

	static Character[] colons = {'\ufe55', ':', '\uff1a'};
	static Pattern[] colonsPat = null;
	static Pattern[] colonsWhitePat = null;

	private static String processColons(String ans, String numPat) {
		/*
     ':' 1. if "5:6" then put together
         2. if others, separate ':' and others
		 *** Note!! All the "digits" are actually extracted/learned from the training data!!!!
             They are not real "digits" knowledge.
		 *** See /u/nlp/data/chinese-segmenter/Sighan2005/dict/wordlist for the list we extracted.
		 */

		// first , just separate all ':'
		if (colonsPat == null) {
			colonsPat = new Pattern[colons.length];
			for (int i = 0; i < colons.length; i++) {
				Character colon = colons[i];
				colonsPat[i] = Pattern.compile(WHITE + colon + WHITE);
			}
		}

		for (int i = 0; i < colons.length; i++) {
			Character colon = colons[i];
			Pattern p = colonsPat[i];
			Matcher m = p.matcher(ans);
			ans = m.replaceAll(" "+colon+" ");
		}

		if (colonsWhitePat == null) {
			colonsWhitePat = new Pattern[colons.length];
			for (int i = 0; i < colons.length; i++) {
				Character colon = colons[i];
				colonsWhitePat[i] = Pattern.compile("("+numPat+")" + WHITEPLUS + colon + WHITEPLUS + "("+numPat+")");
			}
		}
		// second , combine "5:6" patterns
		for (int i = 0; i < colons.length; i++) {
			Character colon = colons[i];
			Pattern p = colonsWhitePat[i];
			Matcher m = p.matcher(ans);
			while(m.find()) {
				ans = m.replaceAll("$1"+colon+"$2");
				m = p.matcher(ans);
			}
		}
		ans = ans.trim();
		return ans;
	}

	private static final Pattern percentsPat = Pattern.compile(WHITE + "([\uff05%])" + WHITE);
	private static final String percentStr = WHITEPLUS + "([\uff05%])";
	private static Pattern percentsWhitePat; // = null;

	private static String processPercents(String ans, String numPat) {
		//  1. if "6%" then put together
		//  2. if others, separate '%' and others
		// System.err.println("Process percents called!");
		// first , just separate all '%'
		Matcher m = percentsPat.matcher(ans);
		ans = m.replaceAll(" $1 ");

		// second , combine "6%" patterns
		if (percentsWhitePat==null) {
			percentsWhitePat = Pattern.compile("(" + numPat + ")" + percentStr);
		}
		Matcher m2 = percentsWhitePat.matcher(ans);
		ans = m2.replaceAll("$1$2");
		ans = ans.trim();
		return ans;
	}

	private static String processDots(String ans, String numPat) {
		/* all "\d\.\d" patterns */
		String dots = "[\ufe52\u2027\uff0e.]";
		Pattern p = Pattern.compile("("+numPat+")" + WHITEPLUS + "("+dots+")" + WHITEPLUS + "("+numPat+")");
		Matcher m = p.matcher(ans);
		while(m.find()) {
			ans = m.replaceAll("$1$2$3");
			m = p.matcher(ans);
		}

		p = Pattern.compile("("+numPat+")("+dots+")" + WHITEPLUS + "("+numPat+")");
		m = p.matcher(ans);
		while (m.find()) {
			ans = m.replaceAll("$1$2$3");
			m = p.matcher(ans);
		}

		p = Pattern.compile("("+numPat+")" + WHITEPLUS + "("+dots+")("+numPat+")");
		m = p.matcher(ans);
		while(m.find()) {
			ans = m.replaceAll("$1$2$3");
			m = p.matcher(ans);
		}

		ans = ans.trim();
		return ans;
	}

	private static String processCommas(String ans) {
		String numPat = "[0-9\uff10-\uff19]";
		String nonNumPat = "[^0-9\uff10-\uff19]";

		/* all "\d\.\d" patterns */
		String commas = ",";

		//Pattern p = Pattern.compile(WHITE + commas + WHITE);
		ans = ans.replaceAll(",", " , ");
		ans = ans.replaceAll("  ", " ");
		if (DEBUG) EncodingPrintWriter.err.println("ANS (before comma norm): "+ans, "UTF-8");
		Pattern p = Pattern.compile("("+numPat+")" + WHITE + "("+commas+")" + WHITE + "("+numPat+"{3}" + nonNumPat+")");
		// cdm: I added the {3} to be a crude fix so it wouldn't joint back
		// up small numbers.  Only proper thousands markers.  But it's a
		// crude hack, which should be done better.
		// In fact this whole method is horrible and should be done better!
		/* -- cdm: I didn't understand this code, and changed it to what
       -- seemed sane to me: replaceAll replaces them all in one step....
    Matcher m = p.matcher(ans);
    while(m.find()) {
    ans = m.replaceAll("$1$2$3");
      m = p.matcher(ans);
    }
		 */
		/* ++ cdm: The replacement */
		Matcher m = p.matcher(ans);
		if (m.find()) {
			ans = m.replaceAll("$1$2$3");
		}
		/*
    p = Pattern.compile("("+nonNumPat+")" + WHITE + "("+commas+")" + WHITE + "("+numPat+")");
    m = p.matcher(ans);
    while(m.find()) {
      ans = m.replaceAll("$1 $2 $3");
      m = p.matcher(ans);
    }

    p = Pattern.compile("("+numPat+")" + WHITE + "("+commas+")" + WHITE + "("+nonNumPat+")");
    m = p.matcher(ans);
    while(m.find()) {
      ans = m.replaceAll("$1 $2 $3");
      m = p.matcher(ans);
    }

    p = Pattern.compile("("+nonNumPat+")" + WHITE + "("+commas+")" + WHITE + "("+nonNumPat+")");
    m = p.matcher(ans);
    while(m.find()) {
      ans = m.replaceAll("$1 $2 $3");
      m = p.matcher(ans);
    }

		 */

		ans = ans.trim();
		return ans;
	}

	public List<String> returnAnswers(List<CoreLabel> doc)
	{
		//		List<String> out = new ArrayList<String>();
		int testContentIdx=0;
		StringBuilder ans = new StringBuilder(); // the actual output we will return

		CoreLabel wi = null;
		for (Iterator<CoreLabel> wordIter = doc.iterator(); wordIter.hasNext();
		testContentIdx++) {
			CoreLabel pwi = wi;
			wi = wordIter.next();
			//System.err.println(wi);
			boolean originalWhiteSpace = "1".equals(wi.get(SpaceBeforeAnnotation.class));

			//  if the CRF says "START" (segmented), and it's not the first word..
			if (wi.get(AnswerAnnotation.class).equals("1") && !("0".equals(String.valueOf(wi.get(PositionAnnotation.class))))) {
				// check if we need to preserve the "no space" between English
				// characters
				boolean seg = true; // since it's in the "1" condition.. default
				// is to seg
				if (flags.keepEnglishWhitespaces) {
					if (testContentIdx > 0) {
						char prevChar = pwi.get(OriginalCharAnnotation.class).charAt(0);
						char currChar = wi.get(OriginalCharAnnotation.class).charAt(0);
						if (isLetterASCII(prevChar) && isLetterASCII(currChar)) {
							// keep the "non space" before wi
							if (! originalWhiteSpace) {
								seg = false;
							}
						}
					}
				}

				// if there was space and keepAllWhitespaces is true, restore it no matter what
				if (flags.keepAllWhitespaces && originalWhiteSpace) {
					seg = true;
				}
				if (seg) {
					if (originalWhiteSpace) {
						ans.append('\u1924'); // a pretty Limbu character which is later changed to a space
					} else {
						ans.append(' ');
					}
				}
			} else {
				boolean seg = false; // since it's in the "0" condition.. default
				// Changed after conversation with Huihsin.
				//
				// Decided that all words consisting of English/ASCII characters
				// should be separated from the surrounding Chinese characters. -cer
				/* Sun Oct  7 22:14:46 2007 (pichuan)
	             the comment above was from DanC.
	             I changed the code but I think I'm doing the same thing here.
				 */
				if (testContentIdx > 0) {
					char prevChar = pwi.get(OriginalCharAnnotation.class).charAt(0);
					char currChar = wi.get(OriginalCharAnnotation.class).charAt(0);
					if ((prevChar < (char)128) != (currChar < (char)128)) {
						if (ChineseUtils.isNumber(prevChar) && ChineseUtils.isNumber(currChar)) {
							// cdm: you would get here if you had an ASCII number next to a
							// Unihan range number.  Does that happen?  It presumably
							// shouldn't do any harm.... [cdm, oct 2007]
						} else {
							seg = true;
						}
					}
				}

				if (flags.keepEnglishWhitespaces) {
					if (testContentIdx > 0) {
						char prevChar = pwi.get(OriginalCharAnnotation.class).charAt(0);
						char currChar = wi.get(OriginalCharAnnotation.class).charAt(0);
						if (isLetterASCII(prevChar) && isLetterASCII(currChar) ||
								isLetterASCII(prevChar) &&  ChineseUtils.isNumber(currChar) ||
								ChineseUtils.isNumber(prevChar) && isLetterASCII(currChar)) {
							// keep the "space" before wi
							if ("1".equals(wi.get(SpaceBeforeAnnotation.class))) {
								seg = true;
							}
						}
					}
				}

				// if there was space and keepAllWhitespaces is true, restore it no matter what
				if (flags.keepAllWhitespaces) {
					if (!("0".equals(String.valueOf(wi.get(PositionAnnotation.class))))
							&& "1".equals(wi.get(SpaceBeforeAnnotation.class))) {
						seg = true;
					}
				}
				if (seg) {
					ans.append(' ');

				}
			}
			ans.append(wi.get(OriginalCharAnnotation.class)); // wi.get(OriginalCharAnnotation.class)); : word
			//			out.add(wi.get(OriginalCharAnnotation.class));
		}
		String ansStr = ans.toString();
		if (flags.sighanPostProcessing) {
			ansStr = postProcessingAnswer(ansStr);
		}
		String[] out = ansStr.split(" ");
		return Arrays.asList(out);
	}

	String postProcessingAnswerCTB(String ans, boolean keepAllWhitespaces) {
		Character[] puncs = {'\u3001', '\u3002', '\u3003', '\u3008', '\u3009', '\u300a', '\u300b',
				'\u300c', '\u300d', '\u300e', '\u300f', '\u3010', '\u3011', '\u3014',
				'\u3015', '\u0028', '\u0029', '\u0022', '\u003c', '\u003e' };
		//'\u3015', '\u0028', '\u0029', '\u0022', '\u003c', '\u003e', '\uff0c'};
		String numPat = "[0-9\uff10-\uff19]+";
		//    if ( ! keepAllWhitespaces) {  // these should now never delete an original space
		ans = separatePuncs(puncs, ans);
		if (flags != null && !flags.suppressMidDotPostprocessing) {
			ans = gluePunc('\u30fb', ans); // this is a 'connector' - the katakana midDot char
		}
		ans = processColons(ans, numPat);
		ans = processPercents(ans, numPat);
		ans = processDots(ans, numPat);
		ans = processCommas(ans);
		//    }
		ans = ans.trim();
		return ans;
	}

	private static String postProcessingAnswerPK(String ans, boolean keepAllWhitespaces) {
		Character[] puncs = {'\u3001', '\u3002', '\u3003', '\u3008', '\u3009', '\u300a', '\u300b',
				'\u300c', '\u300d', '\u300e', '\u300f', '\u3010', '\u3011', '\u3014',
				'\u3015', '\u2103'};

		ans = separatePuncs(puncs, ans);
		/* Note!! All the "digits" are actually extracted/learned from the training data!!!!
       They are not real "digits" knowledge.
       See /u/nlp/data/chinese-segmenter/Sighan2005/dict/wordlist for the list we extracted
		 */
		String numPat = "[0-9\uff10-\uff19\uff0e\u00b7\u4e00\u5341\u767e]+";
		if (!keepAllWhitespaces) {
			ans = processColons(ans, numPat);
			ans = processPercents(ans, numPat);
			ans = processDots(ans, numPat);
			ans = processCommas(ans);


			/* "\u2014\u2014\u2014" and "\u2026\u2026" should be together */

			String[] puncPatterns = {"\u2014" + WHITE + "\u2014" + WHITE + "\u2014", "\u2026" + WHITE + "\u2026"};
			String[] correctPunc = {"\u2014\u2014\u2014", "\u2026\u2026"};
			//String[] puncPatterns = {"\u2014 \u2014 \u2014", "\u2026 \u2026"};

			for (int i = 0; i < puncPatterns.length; i++) {
				Pattern p = Pattern.compile(WHITE + puncPatterns[i]+ WHITE);
				Matcher m = p.matcher(ans);
				ans = m.replaceAll(" "+correctPunc[i]+" ");
			}
		}
		ans = ans.trim();

		return ans;
	}

	private static String postProcessingAnswerMSR(String ans) {
		ans = separatePuncs(ans);
		return ans;
	}


	private static String postProcessingAnswerAS(String ans) {
		ans = separatePuncs(ans);

		/* Note!! All the "digits" are actually extracted/learned from the training data!!!!
       They are not real "digits" knowledge.
       See /u/nlp/data/chinese-segmenter/Sighan2005/dict/wordlist for the list we extracted
		 */
		String numPat = "[\uff10-\uff19\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341\u767e\u5343]+";

		ans = processColons(ans, numPat);
		ans = processPercents(ans, numPat);
		ans = processDots(ans, numPat);
		ans = processCommas(ans);



		return ans;
	}


	private static String postProcessingAnswerHK(String ans) {
		Character[] puncs = {'\u3001', '\u3002', '\u3003', '\u3008', '\u3009', '\u300a', '\u300b',
				'\u300c', '\u300d', '\u300e', '\u300f', '\u3010', '\u3011', '\u3014',
				'\u3015', '\u2103'};

		ans = separatePuncs(puncs, ans);

		/* Note!! All the "digits" are actually extracted/learned from the training data!!!!
       They are not real "digits" knowledge.
       See /u/nlp/data/chinese-segmenter/Sighan2005/dict/wordlist for the list we extracted
		 */
		String numPat = "[0-9]+";
		ans = processColons(ans, numPat);


		/* "\u2014\u2014\u2014" and "\u2026\u2026" should be together */

		String[] puncPatterns = {"\u2014" + WHITE + "\u2014" + WHITE + "\u2014", "\u2026" + WHITE + "\u2026"};
		String[] correctPunc = {"\u2014\u2014\u2014", "\u2026\u2026"};
		//String[] puncPatterns = {"\u2014 \u2014 \u2014", "\u2026 \u2026"};

		for (int i = 0; i < puncPatterns.length; i++) {
			Pattern p = Pattern.compile(WHITE + puncPatterns[i]+ WHITE);
			Matcher m = p.matcher(ans);
			ans = m.replaceAll(" "+correctPunc[i]+" ");
		}
		ans = ans.trim();


		return ans;
	}

	private static String intern(String s) {
		return s.trim().intern();
	}

	public void printLattice(DFSA tagLattice, List<CoreLabel> doc, PrintWriter out) {
		CoreLabel[] docArray = doc.toArray(new CoreLabel[doc.size()]);
		// Create answer lattice:
		MutableInteger nodeId = new MutableInteger(0);
		DFSA answerLattice = new DFSA(null);
		DFSAState aInitState = new DFSAState(nodeId.intValue(),answerLattice);
		answerLattice.setInitialState(aInitState);
		Map<DFSAState,DFSAState> stateLinks = new HashMap<DFSAState,DFSAState>();
		// Convert binary lattice into word lattice:
		tagLatticeToAnswerLattice
		(tagLattice.initialState(), aInitState, new StringBuilder(""), nodeId, 0, 0.0, stateLinks, answerLattice, docArray);
		try {
			answerLattice.printAttFsmFormat(out);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Recursively builds an answer lattice (Chinese words) from a Viterbi search graph
	 * of binary predictions. This function does a limited amount of post-processing:
	 * preserve white spaces of the input, and not segment between two latin characters or
	 * between two digits. Consequently, the probabilities of all paths in anserLattice
	 * may not sum to 1 (they do sum to 1 if no post processing applies).
	 *
	 * @arg tSource Current node in Viterbi search graph.
	 * @arg aSource Current node in answer lattice.
	 * @arg answer Partial word starting at aSource.
	 * @arg nodeId Currently unused node identifier for answer graph.
	 * @arg pos Current position in docArray.
	 * @arg cost Current cost of answer.
	 * @arg stateLinks Maps nodes of the search graph to nodes in answer lattice
	 * (when paths of the search graph are recombined, paths of the answer lattice should be
	 *  recombined as well, if at word boundary).
	 */
	private void tagLatticeToAnswerLattice
	(DFSAState tSource, DFSAState aSource, StringBuilder answer,
			MutableInteger nodeId, int pos, double cost,
			Map<DFSAState,DFSAState> stateLinks, DFSA answerLattice, CoreLabel[] docArray) {
		// Add "1" prediction after the end of the sentence, if applicable:
		if(tSource.isAccepting() && tSource.continuingInputs().size() == 0) {
			tSource.addTransition
			(new DFSATransition("", tSource, new DFSAState(-1, null), "1", "", 0));
		}
		// Get current label, character, and prediction:
		CoreLabel curLabel = (pos < docArray.length) ? docArray[pos] : null;
		String curChr = null, origSpace = null;
		if(curLabel != null) {
			curChr = curLabel.get(OriginalCharAnnotation.class);
			assert(curChr.length() == 1);
			origSpace = curLabel.get(SpaceBeforeAnnotation.class);
		}
		// Get set of successors in search graph:
		Set inputs = tSource.continuingInputs();
		// Only keep most probable transition out of initial state:
		Object answerConstraint = null;
		if(pos == 0) {
			double minCost = Double.POSITIVE_INFINITY;
			DFSATransition bestTransition = null;
			for (Iterator iter = inputs.iterator(); iter.hasNext();) {
				Object predictSpace = iter.next();
				DFSATransition transition = tSource.transition(predictSpace);
				double transitionCost = transition.score();
				if(transitionCost < minCost) {
					if(predictSpace != null) {
						System.err.printf("mincost (%s): %e -> %e\n", predictSpace.toString(), minCost, transitionCost);
						minCost = transitionCost;
						answerConstraint = predictSpace;
					}
				}
			}
		}
		// Follow along each transition:
		for (Iterator iter = inputs.iterator(); iter.hasNext();) {
			Object predictSpace = iter.next();
			DFSATransition transition = tSource.transition(predictSpace);
			DFSAState tDest = transition.target();
			DFSAState newASource = aSource;
			//System.err.printf("tsource=%s tdest=%s asource=%s pos=%d predictSpace=%s\n", tSource, tDest, newASource, pos, predictSpace);
			StringBuilder newAnswer = new StringBuilder(answer.toString());
			int answerLen = newAnswer.length();
			String prevChr = (answerLen > 0) ? newAnswer.substring(answerLen-1) : null;
			double newCost = cost;
			// Ignore paths starting with zero:
			if(answerConstraint != null && !answerConstraint.equals(predictSpace)) {
				System.err.printf("Skipping transition %s at pos 0.\n",predictSpace.toString());
				continue;
			}
			// Ignore paths not consistent with input segmentation:
			if(flags.keepAllWhitespaces && "0".equals(predictSpace) && "1".equals(origSpace)) {
				System.err.printf("Skipping non-boundary at pos %d, since space in the input.\n",pos);
				continue;
			}
			// Ignore paths adding segment boundaries between two latin characters, or between two digits:
			// (unless already present in original input)
			if("1".equals(predictSpace) && "0".equals(origSpace) && prevChr != null && curChr != null) {
				char p = prevChr.charAt(0), c = curChr.charAt(0);
				if(isLetterASCII(p) && isLetterASCII(c)) {
					System.err.printf("Not hypothesizing a boundary at pos %d, since between two ASCII letters (%s and %s).\n",
							pos,prevChr,curChr);
					continue;
				}
				if(ChineseUtils.isNumber(p) && ChineseUtils.isNumber(c)) {
					System.err.printf("Not hypothesizing a boundary at pos %d, since between two numeral characters (%s and %s).\n",
							pos,prevChr,curChr);
					continue;
				}
			}
			// If predictSpace==1, create a new transition in answer search graph:
			if("1".equals(predictSpace)) {
				if(newAnswer.toString().length() > 0) {
					// If answer destination node visited before, create a new edge and leave:
					if(stateLinks.containsKey(tSource)) {
						DFSAState aDest = stateLinks.get(tSource);
						newASource.addTransition
						(new DFSATransition("", newASource, aDest, newAnswer.toString(), "", newCost));
						//System.err.printf("new transition: asource=%s adest=%s edge=%s\n", newASource, aDest, newAnswer.toString());
						continue;
					}
					// If answer destination node not visited before, create it + new edge:
					nodeId.incValue(1);
					DFSAState aDest = new DFSAState(nodeId.intValue(), answerLattice, 0.0);
					stateLinks.put(tSource,aDest);
					newASource.addTransition(new DFSATransition("", newASource, aDest, newAnswer.toString(), "", newCost));
					//System.err.printf("new edge: adest=%s\n", newASource, aDest, newAnswer.toString());
					//System.err.printf("new transition: asource=%s adest=%s edge=%s\n\n\n", newASource, aDest, newAnswer.toString());
					// Reached an accepting state:
					if(tSource.isAccepting()) {
						aDest.setAccepting(true);
						continue;
					}
					// Start new answer edge:
					newASource = aDest;
					newAnswer = new StringBuilder();
					newCost = 0.0;
				}
			}
			assert(curChr != null);
			newAnswer.append(curChr);
			newCost += transition.score();
			if(newCost < flags.searchGraphPrune || isLetterASCII(curChr.charAt(0)))
				tagLatticeToAnswerLattice(tDest, newASource, newAnswer, nodeId, pos+1, newCost, stateLinks, answerLattice, docArray);
		}
	}

	/**
	 * just for testing
	 */
	public static void main(String[] args) {
		String input = args[0];
		String enc = args[1];

		for (String line : ObjectBank.getLineIterator(new File(input), enc)) {
			// System.out.println(postProcessingAnswerHK(line));
			EncodingPrintWriter.out.println(processPercents(line, "[0-9\uff10-\uff19]+"), "UTF-8");
		}
	}

	private static final long serialVersionUID = 3260295150250263237L;

}
