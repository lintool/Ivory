// AbstractSequenceClassifier -- a framework for probabilistic sequence models.
// Copyright (c) 2002-2008 The Board of Trustees of
// The Leland Stanford Junior University. All Rights Reserved.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
//
// For more information, bug reports, fixes, contact:
//    Christopher Manning
//    Dept of Computer Science, Gates 1A
//    Stanford CA 94305-9010
//    USA
//    Support/Questions: java-nlp-user@lists.stanford.edu
//    Licensing: java-nlp-support@lists.stanford.edu
//    http://nlp.stanford.edu/downloads/crf-classifier.shtml

package edu.stanford.nlp.ie;

import edu.stanford.nlp.fsm.DFSA;
import edu.stanford.nlp.io.RegExFileFilter;
import edu.stanford.nlp.ling.CoreAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.*;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.objectbank.ObjectBank;
import edu.stanford.nlp.objectbank.ResettableReaderIteratorFactory;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.CoreTokenFactory;
import edu.stanford.nlp.sequences.*;
import edu.stanford.nlp.stats.ClassicCounter;
import edu.stanford.nlp.stats.Counter;
import edu.stanford.nlp.stats.Counters;
import edu.stanford.nlp.stats.Sampler;
import edu.stanford.nlp.trees.international.pennchinese.ChineseUtils;
import edu.stanford.nlp.util.*;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * This class provides common functionality for (probabilistic) sequence models.
 * It is a superclass of our CMM and CRF sequence classifiers, and is even used
 * in the (deterministic) NumberSequenceClassifier. See implementing classes for
 * more information.
 * <p>
 * A full implementation should implement these 5 abstract methods:
 * List&lt;CoreLabel&gt; classify(List&lt;CoreLabel&gt; document); void
 * train(Collection&lt;List&lt;CoreLabel&gt;&gt; docs);
 * printProbsDocument(List&lt;CoreLabel&gt; document); void
 * serializeClassifier(String serializePath); void
 * loadClassifier(ObjectInputStream in, Properties props) throws IOException,
 * ClassCastException, ClassNotFoundException; but a runtime (or rule-based)
 * implementation can usefully implement just the first.
 *
 * @author Jenny Finkel
 * @author Dan Klein
 * @author Christopher Manning
 * @author Dan Cer
 * @author sonalg (made the class generic)
 */
public abstract class AbstractSequenceClassifier<IN extends CoreMap> implements Function<String, String> {

	public SeqClassifierFlags flags;
	public Index<String> classIndex; // = null;
	public FeatureFactory<IN> featureFactory;
	protected IN pad;
	private CoreTokenFactory<IN> tokenFactory;
	protected int windowSize;
	// different threads can add or query knownLCWords at the same time,
	// so we need a concurrent data structure
	protected Set<String> knownLCWords = new ConcurrentHashSet<String>();

	/**
	 * Construct a SeqClassifierFlags object based on the passed in properties,
	 * and then call the other constructor.
	 *
	 * @param props
	 *          See SeqClassifierFlags for known properties.
	 */
	public AbstractSequenceClassifier(Properties props) {
		this(new SeqClassifierFlags(props));
	}

	/**
	 * Initialize the featureFactory and other variables based on the passed in
	 * flags.
	 *
	 * @param flags
	 *          A specification of the AbstractSequenceClassifier to construct.
	 */
	public AbstractSequenceClassifier(SeqClassifierFlags flags) {
		this.flags = flags;

		try {
			this.featureFactory = (FeatureFactory) Class.forName(flags.featureFactory).newInstance();
			if (flags.tokenFactory == null)
				tokenFactory = (CoreTokenFactory<IN>) new CoreLabelTokenFactory();
			else
				this.tokenFactory = (CoreTokenFactory<IN>) Class.forName(flags.tokenFactory).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		pad = tokenFactory.makeToken();
		windowSize = flags.maxLeft + 1;
		reinit();
	}

	/**
	 * This method should be called after there have been changes to the flags
	 * (SeqClassifierFlags) variable, such as after deserializing a classifier. It
	 * is called inside the loadClassifier methods. It assumes that the flags
	 * variable and the pad variable exist, but reinitializes things like the pad
	 * variable, featureFactory and readerAndWriter based on the flags.
	 * <p>
	 * <i>Implementation note:</i> At the moment this variable doesn't set
	 * windowSize or featureFactory, since they are being serialized separately in
	 * the file, but we should probably stop serializing them and just
	 * reinitialize them from the flags?
	 */
	protected final void reinit() {
		pad.set(AnswerAnnotation.class, flags.backgroundSymbol);
		pad.set(GoldAnswerAnnotation.class, flags.backgroundSymbol);

		featureFactory.init(flags);

	}

	public DocumentReaderAndWriter
	makeReaderAndWriter() {
		DocumentReaderAndWriter readerAndWriter;
		try {
			readerAndWriter = ((DocumentReaderAndWriter)
					Class.forName(flags.readerAndWriter).newInstance());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		readerAndWriter.init(flags);
		return readerAndWriter;
	}

	public DocumentReaderAndWriter
	makeReaderAndWriter(FSDataInputStream stream) {
		DocumentReaderAndWriter readerAndWriter;
		try {
			readerAndWriter =  ((DocumentReaderAndWriter)
					Class.forName(flags.readerAndWriter).newInstance());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		readerAndWriter.init(stream, flags);
		return readerAndWriter;
	}
	
	/**
	 * Returns the background class for the classifier.
	 *
	 * @return The background class name
	 */
	public String backgroundSymbol() {
		return flags.backgroundSymbol;
	}

	public Set<String> labels() {
		return new HashSet<String>(classIndex.objectsList());
	}

	/**
	 * Classify a List of IN. This method returns a new list of tokens, not
	 * the list of tokens passed in, and runs the new tokens through
	 * ObjectBankWrapper.  (Both these behaviors are different from that of the
	 * classify(List) method.
	 *
	 * @param sentence The List of IN to be classified.
	 * @return The classified List of IN, where the classifier output for
	 *         each token is stored in its {@link AnswerAnnotation} field.
	 */
	public List<IN> classifySentence(List<? extends HasWord> sentence) {
		List<IN> document = new ArrayList<IN>();
		int i = 0;
		for (HasWord word : sentence) {
			IN wi = null;
			if (word instanceof CoreMap) {
				// copy all annotations! some are required later in
				// AbstractSequenceClassifier.classifyWithInlineXML
				// wi = (IN) new ArrayCoreMap((ArrayCoreMap) word);
				wi = tokenFactory.makeToken((IN) word);
			} else {
				wi = tokenFactory.makeToken();
				wi.set(CoreAnnotations.TextAnnotation.class, word.word());
				// wi.setWord(word.word());
			}
			wi.set(PositionAnnotation.class, Integer.toString(i));
			wi.set(AnswerAnnotation.class, backgroundSymbol());
			document.add(wi);
			i++;
		}

		// TODO get rid of objectbankwrapper
		ObjectBankWrapper<IN> wrapper = new ObjectBankWrapper<IN>(flags, null, knownLCWords);
		wrapper.processDocument(document);

		classify(document);

		return document;
	}

	/**
	 * Classify a List of IN using whatever additional information is passed in globalInfo.
	 * Used by SUTime (NumberSequenceClassifier), which requires the doc date to resolve relative dates
	 *
	 * @param tokenSequence
	 *          The List of IN to be classified.
	 * @return The classified List of IN, where the classifier output for
	 *         each token is stored in its "answer" field.
	 */
	public List<IN> classifySentenceWithGlobalInformation(List<? extends HasWord> tokenSequence, final CoreMap doc, final CoreMap sentence) {
		List<IN> document = new ArrayList<IN>();
		int i = 0;
		for (HasWord word : tokenSequence) {
			IN wi = null;
			if (word instanceof CoreMap) {
				// copy all annotations! some are required later in
				// AbstractSequenceClassifier.classifyWithInlineXML
				// wi = (IN) new ArrayCoreMap((ArrayCoreMap) word);
				wi = tokenFactory.makeToken((IN) word);
			} else {
				wi = tokenFactory.makeToken();
				wi.set(CoreAnnotations.TextAnnotation.class, word.word());
				// wi.setWord(word.word());
			}
			wi.set(PositionAnnotation.class, Integer.toString(i));
			wi.set(AnswerAnnotation.class, backgroundSymbol());
			document.add(wi);
			i++;
		}

		// TODO get rid of objectbankwrapper
		ObjectBankWrapper<IN> wrapper = new ObjectBankWrapper<IN>(flags, null, knownLCWords);
		wrapper.processDocument(document);

		classifyWithGlobalInformation(document, doc, sentence);

		return document;
	}

	public SequenceModel getSequenceModel(List<IN> doc) {
		throw new UnsupportedOperationException();
	}

	public Sampler<List<IN>> getSampler(final List<IN> input) {
		return new Sampler<List<IN>>() {
			SequenceModel model = getSequenceModel(input);
			SequenceSampler sampler = new SequenceSampler();

			public List<IN> drawSample() {
				int[] sampleArray = sampler.bestSequence(model);
				List<IN> sample = new ArrayList<IN>();
				int i = 0;
				for (IN word : input) {

					IN newWord = tokenFactory.makeToken(word);
					newWord.set(AnswerAnnotation.class, classIndex.get(sampleArray[i++]));
					sample.add(newWord);
				}
				return sample;
			}
		};
	}

	public Counter<List<IN>> classifyKBest(List<IN> doc, Class<? extends CoreAnnotation<String>> answerField, int k) {

		if (doc.isEmpty()) {
			return new ClassicCounter<List<IN>>();
		}

		// TODO get rid of ObjectBankWrapper
		// i'm sorry that this is so hideous - JRF
		ObjectBankWrapper<IN> obw = new ObjectBankWrapper<IN>(flags, null, knownLCWords);
		doc = obw.processDocument(doc);

		SequenceModel model = getSequenceModel(doc);

		KBestSequenceFinder tagInference = new KBestSequenceFinder();
		Counter<int[]> bestSequences = tagInference.kBestSequences(model, k);

		Counter<List<IN>> kBest = new ClassicCounter<List<IN>>();

		for (int[] seq : bestSequences.keySet()) {
			List<IN> kth = new ArrayList<IN>();
			int pos = model.leftWindow();
			for (IN fi : doc) {
				IN newFL = tokenFactory.makeToken(fi);
				String guess = classIndex.get(seq[pos]);
				fi.remove(AnswerAnnotation.class); // because fake answers will get
				// added during testing
				newFL.set(answerField, guess);
				pos++;
				kth.add(newFL);
			}
			kBest.setCount(kth, bestSequences.getCount(seq));
		}

		return kBest;
	}

	public DFSA<String, Integer> getViterbiSearchGraph(List<IN> doc, Class<? extends CoreAnnotation<String>> answerField) {
		if (doc.isEmpty()) {
			return new DFSA<String, Integer>(null);
		}
		// TODO get rid of objectbankwrapper
		ObjectBankWrapper<IN> obw = new ObjectBankWrapper<IN>(flags, null, knownLCWords);
		doc = obw.processDocument(doc);
		SequenceModel model = getSequenceModel(doc);
		return ViterbiSearchGraphBuilder.getGraph(model, classIndex);
	}

	/**
	 * Classify the tokens in a String. Each sentence becomes a separate document.
	 *
	 * @param str
	 *          A String with tokens in one or more sentences of text to be
	 *          classified.
	 * @return {@link List} of classified sentences (each a List of something that
	 *         extends {@link CoreMap}).
	 */
	public List<List<IN>> classify(String str) {
		DocumentReaderAndWriter<IN> readerAndWriter =
			new PlainTextDocumentReaderAndWriter<IN>();
		readerAndWriter.init(flags);
		ObjectBank<List<IN>> documents =
			makeObjectBankFromString(str, readerAndWriter);
		List<List<IN>> result = new ArrayList<List<IN>>();

		for (List<IN> document : documents) {
			classify(document);

			List<IN> sentence = new ArrayList<IN>();
			for (IN wi : document) {
				// TaggedWord word = new TaggedWord(wi.word(), wi.answer());
				// sentence.add(word);
				sentence.add(wi);
			}
			result.add(sentence);
		}
		return result;
	}

	/**
	 * Classify the tokens in a String. Each sentence becomes a separate document.
	 * Doesn't override default readerAndWriter.
	 *
	 * @param str
	 *          A String with tokens in one or more sentences of text to be
	 *          classified.
	 * @return {@link List} of classified sentences (each a List of something that
	 *         extends {@link CoreMap}).
	 */
	public List<List<IN>> classifyRaw(String str,
			DocumentReaderAndWriter readerAndWriter) {
		ObjectBank<List<IN>> documents =
			makeObjectBankFromString(str, readerAndWriter);
		List<List<IN>> result = new ArrayList<List<IN>>();

		for (List<IN> document : documents) {
			classify(document);

			List<IN> sentence = new ArrayList<IN>();
			for (IN wi : document) {
				// TaggedWord word = new TaggedWord(wi.word(), wi.answer());
				// sentence.add(word);
				sentence.add(wi);
			}
			result.add(sentence);
		}
		return result;
	}

	/**
	 * Classify the contents of a file.
	 *
	 * @param filename
	 *          Contains the sentence(s) to be classified.
	 * @return {@link List} of classified List of IN.
	 */
	public List<List<IN>> classifyFile(String filename) {
		DocumentReaderAndWriter<IN> readerAndWriter =
			new PlainTextDocumentReaderAndWriter<IN>();
		readerAndWriter.init(flags);
		ObjectBank<List<IN>> documents =
			makeObjectBankFromFile(filename, readerAndWriter);
		List<List<IN>> result = new ArrayList<List<IN>>();

		for (List<IN> document : documents) {
			// System.err.println(document);
			classify(document);

			List<IN> sentence = new ArrayList<IN>();
			for (IN wi : document) {
				sentence.add(wi);
				// System.err.println(wi);
			}
			result.add(sentence);
		}
		return result;
	}

	/**
	 * Maps a String input to an XML-formatted rendition of applying NER to the
	 * String. Implements the Function interface. Calls
	 * classifyWithInlineXML(String) [q.v.].
	 */
	public String apply(String in) {
		return classifyWithInlineXML(in);
	}

	/**
	 * Classify the contents of a {@link String} to one of several String
	 * representations that shows the classes. Plain text or XML input is expected
	 * and the {@link PlainTextDocumentReaderAndWriter} is used. The classifier
	 * will tokenize the text and treat each sentence as a separate document. The
	 * output can be specified to be in a choice of three formats: slashTags
	 * (e.g., Bill/PERSON Smith/PERSON died/O ./O), inlineXML (e.g.,
	 * &lt;PERSON&gt;Bill Smith&lt;/PERSON&gt; went to
	 * &lt;LOCATION&gt;Paris&lt;/LOCATION&gt; .), or xml, for stand-off XML (e.g.,
	 * &lt;wi num="0" entity="PERSON"&gt;Sue&lt;/wi&gt; &lt;wi num="1"
	 * entity="O"&gt;shouted&lt;/wi&gt; ). There is also a binary choice as to
	 * whether the spacing between tokens of the original is preserved or whether
	 * the (tagged) tokens are printed with a single space (for inlineXML or
	 * slashTags) or a single newline (for xml) between each one.
	 * <p>
	 * <i>Fine points:</i> The slashTags and xml formats show tokens as
	 * transformed by any normalization processes inside the tokenizer, while
	 * inlineXML shows the tokens exactly as they appeared in the source text.
	 * When a period counts as both part of an abbreviation and as an end of
	 * sentence marker, it is included twice in the output String for slashTags or
	 * xml, but only once for inlineXML, where it is not counted as part of the
	 * abbreviation (or any named entity it is part of). For slashTags with
	 * preserveSpacing=true, there will be two successive periods such as "Jr.."
	 * The tokenized (preserveSpacing=false) output will have a space or a newline
	 * after the last token.
	 *
	 * @param sentences
	 *          The String to be classified. It will be tokenized and
	 *          divided into documents according to (heuristically
	 *          determined) sentence boundaries.
	 * @param outputFormat
	 *          The format to put the output in: one of "slashTags", "xml", or
	 *          "inlineXML"
	 * @param preserveSpacing
	 *          Whether to preserve the input spacing between tokens, which may
	 *          sometimes be none (true) or whether to tokenize the text and print
	 *          it with one space between each token (false)
	 * @return A {@link String} with annotated with classification information.
	 */
	public String classifyToString(String sentences, String outputFormat, boolean preserveSpacing) {
		PlainTextDocumentReaderAndWriter.OutputStyle outFormat =
			PlainTextDocumentReaderAndWriter.OutputStyle.fromShortName(outputFormat);

		PlainTextDocumentReaderAndWriter<IN> readerAndWriter =
			new PlainTextDocumentReaderAndWriter<IN>();
		readerAndWriter.init(flags);

		ObjectBank<List<IN>> documents =
			makeObjectBankFromString(sentences, readerAndWriter);

		StringBuilder sb = new StringBuilder();
		for (List<IN> doc : documents) {
			List<IN> docOutput = classify(doc);
			sb.append(readerAndWriter.getAnswers(docOutput, outFormat,
					preserveSpacing));
		}
		return sb.toString();
	}

	/**
	 * Classify the contents of a {@link String}. Plain text or XML is expected
	 * and the {@link PlainTextDocumentReaderAndWriter} is used. The classifier
	 * will treat each sentence as a separate document. The output can be
	 * specified to be in a choice of formats: Output is in inline XML format
	 * (e.g. &lt;PERSON&gt;Bill Smith&lt;/PERSON&gt; went to
	 * &lt;LOCATION&gt;Paris&lt;/LOCATION&gt; .)
	 *
	 * @param sentences
	 *          The string to be classified
	 * @return A {@link String} with annotated with classification information.
	 */
	public String classifyWithInlineXML(String sentences) {
		return classifyToString(sentences, "inlineXML", true);
	}

	/**
	 * Classify the contents of a String to a tagged word/class String. Plain text
	 * or XML input is expected and the {@link PlainTextDocumentReaderAndWriter}
	 * is used. Output looks like: My/O name/O is/O Bill/PERSON Smith/PERSON ./O
	 *
	 * @param sentences
	 *          The String to be classified
	 * @return A String annotated with classification information.
	 */
	public String classifyToString(String sentences) {
		return classifyToString(sentences, "slashTags", true);
	}

	/**
	 * Classify the contents of a {@link String} to classified character offset
	 * spans. Plain text or XML input text is expected and the
	 * {@link PlainTextDocumentReaderAndWriter} is used. Output is a (possibly
	 * empty, but not <code>null</code>) List of Triples. Each Triple is an entity
	 * name, followed by beginning and ending character offsets in the original
	 * String. Character offsets can be thought of as fenceposts between the
	 * characters, or, like certain methods in the Java String class, as character
	 * positions, numbered starting from 0, with the end index pointing to the
	 * position AFTER the entity ends. That is, end - start is the length of the
	 * entity in characters.
	 * <p>
	 * <i>Fine points:</i> Token offsets are true wrt the source text, even though
	 * the tokenizer may internally normalize certain tokens to String
	 * representations of different lengths (e.g., " becoming `` or ''). When a
	 * period counts as both part of an abbreviation and as an end of sentence
	 * marker, and that abbreviation is part of a named entity, the reported
	 * entity string excludes the period.
	 *
	 * @param sentences
	 *          The string to be classified
	 * @return A {@link List} of {@link Triple}s, each of which gives an entity
	 *         type and the beginning and ending character offsets.
	 */
	public List<Triple<String, Integer, Integer>> classifyToCharacterOffsets(String sentences) {
		DocumentReaderAndWriter<IN> readerAndWriter =
			new PlainTextDocumentReaderAndWriter<IN>();
		readerAndWriter.init(flags);
		ObjectBank<List<IN>> documents =
			makeObjectBankFromString(sentences, readerAndWriter);

		List<Triple<String, Integer, Integer>> entities =
			new ArrayList<Triple<String, Integer, Integer>>();
		for (List<IN> doc : documents) {
			String prevEntityType = flags.backgroundSymbol;
			Triple<String, Integer, Integer> prevEntity = null;

			classify(doc);

			for (IN fl : doc) {
				String guessedAnswer = fl.get(AnswerAnnotation.class);
				if (guessedAnswer.equals(flags.backgroundSymbol)) {
					if (prevEntity != null) {
						entities.add(prevEntity);
						prevEntity = null;
					}
				} else {
					if (!guessedAnswer.equals(prevEntityType)) {
						if (prevEntity != null) {
							entities.add(prevEntity);
						}
						prevEntity = new Triple<String, Integer, Integer>(guessedAnswer, fl
								.get(CharacterOffsetBeginAnnotation.class), fl.get(CharacterOffsetEndAnnotation.class));
					} else {
						assert prevEntity != null; // if you read the code carefully, this
						// should always be true!
						prevEntity.setThird(fl.get(CharacterOffsetEndAnnotation.class));
					}
				}
				prevEntityType = guessedAnswer;
			}

			// include any entity at end of doc
			if (prevEntity != null) {
				entities.add(prevEntity);
			}

		}
		return entities;
	}

	/**
	 * ONLY USE IF LOADED A CHINESE WORD SEGMENTER!!!!!
	 *
	 * @param sentence
	 *          The string to be classified
	 * @return List of words
	 */
	public List<String> segmentString(String sentence) {
		// TODO: creating this object is annoying, is there any good way
		// around it?  Why can't we just use a
		// PlainTextDocumentReaderAndWriter?
		return segmentString(sentence, makeReaderAndWriter());
	}

	public List<String> segmentString(String sentence,
			DocumentReaderAndWriter<IN> readerAndWriter) {
		ObjectBank<List<IN>> docs = makeObjectBankFromString(sentence,
				readerAndWriter);

		StringWriter stringWriter = new StringWriter();
		PrintWriter stringPrintWriter = new PrintWriter(stringWriter);
		for (List<IN> doc : docs) {
			classify(doc);
			readerAndWriter.printAnswers(doc, stringPrintWriter);
			stringPrintWriter.println();
		}
		stringPrintWriter.close();
		String segmented = stringWriter.toString();

		return Arrays.asList(segmented.split("\\s"));
	}

	/**
	 * Classify the contents of {@link SeqClassifierFlags scf.testFile}. The file
	 * should be in the format expected based on {@link SeqClassifierFlags
	 * scf.documentReader}.
	 *
	 * @return A {@link List} of {@link List}s of classified something that
	 *         extends {@link CoreMap} where each {@link List} refers to a
	 *         document/sentence.
	 */
	// public ObjectBank<List<IN>> test() {
	// return test(flags.testFile);
	// }

	/**
	 * Classify the contents of a file. The file should be in the format expected
	 * based on {@link SeqClassifierFlags scf.documentReader} if the file is
	 * specified in {@link SeqClassifierFlags scf.testFile}. If the file being
	 * read is from {@link SeqClassifierFlags scf.textFile} then the
	 * {@link PlainTextDocumentReaderAndWriter} is used.
	 *
	 * @param filename
	 *          The path to the specified file
	 * @return A {@link List} of {@link List}s of classified {@link IN}s where
	 *         each {@link List} refers to a document/sentence.
	 */
	// public ObjectBank<List<IN>> test(String filename) {
	// // only for the OCR data does this matter
	// flags.ocrTrain = false;

	// ObjectBank<List<IN>> docs = makeObjectBank(filename);
	// return testDocuments(docs);
	// }

	/**
	 * Classify a {@link List} of something that extends{@link CoreMap}.
	 * The classifications are added in place to the items of the document,
	 * which is also returned by this method
	 *
	 * @param document A {@link List} of something that extends {@link CoreMap}.
	 * @return The same {@link List}, but with the elements annotated with their
	 *         answers (stored under the {@link AnswerAnnotation} key).
	 */
	public abstract List<IN> classify(List<IN> document);

	/**
	 * Classify a {@link List} of something that extends{@link CoreMap} using as additional information whatever is stored in globalInfo.
	 * This needed for SUTime (NumberSequenceClassifier), which requires the document date to resolve relative dates
	 * @param document
	 * @param globalInfo
	 * @return
	 */
	public abstract List<IN> classifyWithGlobalInformation(List<IN> tokenSequence, final CoreMap document, final CoreMap sentence);

	/**
	 * Train the classifier based on values in flags. It will use the first of
	 * these variables that is defined: trainFiles (and baseTrainDir),
	 * trainFileList, trainFile.
	 */
	public void train() {
		if (flags.trainFiles != null) {
			train(flags.baseTrainDir, flags.trainFiles, makeReaderAndWriter());
		} else if (flags.trainFileList != null) {
			String[] files = flags.trainFileList.split(",");
			train(files, makeReaderAndWriter());
		} else {
			train(flags.trainFile, makeReaderAndWriter());
		}
	}

	public void train(String filename) {
		train(filename, makeReaderAndWriter());
	}

	public void train(String filename,
			DocumentReaderAndWriter readerAndWriter) {
		// only for the OCR data does this matter
		flags.ocrTrain = true;
		train(makeObjectBankFromFile(filename, readerAndWriter), readerAndWriter);
	}

	public void train(String baseTrainDir, String trainFiles,
			DocumentReaderAndWriter readerAndWriter) {
		// only for the OCR data does this matter
		flags.ocrTrain = true;
		train(makeObjectBankFromFiles(baseTrainDir, trainFiles, readerAndWriter),
				readerAndWriter);
	}

	public void train(String[] trainFileList,
			DocumentReaderAndWriter readerAndWriter) {
		// only for the OCR data does this matter
		flags.ocrTrain = true;
		train(makeObjectBankFromFiles(trainFileList, readerAndWriter),
				readerAndWriter);
	}

	/**
	 * Trains a classifier from a Collection of sequences.
	 * Note that the Collection can be (and usually is) an ObjectBank.
	 *
	 * @param docs
	 *          An Objectbank or a collection of sequences of IN
	 */
	public void train(Collection<List<IN>> docs) {
		train(docs, makeReaderAndWriter());
	}

	/**
	 * Trains a classifier from a Collection of sequences.
	 * Note that the Collection can be (and usually is) an ObjectBank.
	 *
	 * @param docs
	 *          An Objectbank or a collection of sequences of IN
	 * @param readerAndWriter
	 *          A DocumentReaderAndWriter to use when loading test files
	 */
	public abstract void train(Collection<List<IN>> docs,
			DocumentReaderAndWriter readerAndWriter);

	/**
	 * Reads a String into an ObjectBank object. NOTE: that the current
	 * implementation of ReaderIteratorFactory will first try to interpret each
	 * string as a filename, so this method will yield unwanted results if it
	 * applies to a string that is at the same time a filename. It prints out a
	 * warning, at least.
	 *
	 * @param string
	 *          The String which will be the content of the ObjectBank
	 * @return The ObjectBank
	 */
	public ObjectBank<List<IN>>
	makeObjectBankFromString(String string,
			DocumentReaderAndWriter readerAndWriter)
			{
		if (flags.announceObjectBankEntries) {
			System.err.print("Reading data using ");
			System.err.println(flags.readerAndWriter);

			if (flags.inputEncoding == null) {
				System.err.println("Getting data from " + string + " (default encoding)");
			} else {
				System.err.println("Getting data from " + string + " (" + flags.inputEncoding + " encoding)");
			}
		}
		// return new ObjectBank<List<IN>>(new
		// ResettableReaderIteratorFactory(string), readerAndWriter);
		// TODO
		return new ObjectBankWrapper<IN>(flags, new ObjectBank<List<IN>>(new ResettableReaderIteratorFactory(string),
				readerAndWriter), knownLCWords);
			}

	public ObjectBank<List<IN>> makeObjectBankFromFile(String filename,
			DocumentReaderAndWriter readerAndWriter) {
		String[] fileAsArray = { filename };
		return makeObjectBankFromFiles(fileAsArray, readerAndWriter);
	}

	public ObjectBank<List<IN>> makeObjectBankFromFiles(String[] trainFileList,
			DocumentReaderAndWriter readerAndWriter) {
		// try{
			Collection<File> files = new ArrayList<File>();
			for (String trainFile : trainFileList) {
				File f = new File(trainFile);
				files.add(f);
			}
			// System.err.printf("trainFileList contains %d file%s.\n", files.size(),
			// files.size() == 1 ? "": "s");
			// TODO get rid of objectbankwrapper
			// return new ObjectBank<List<IN>>(new
			// ResettableReaderIteratorFactory(files), readerAndWriter);
			return new ObjectBankWrapper<IN>(flags, new ObjectBank<List<IN>>(new ResettableReaderIteratorFactory(files, flags.inputEncoding),
					readerAndWriter), knownLCWords);
			// } catch (IOException e) {
			// throw new RuntimeException(e);
			// }
	}

	public ObjectBank<List<IN>> makeObjectBankFromFiles(String baseDir, String filePattern, DocumentReaderAndWriter readerAndWriter) {

		File path = new File(baseDir);
		FileFilter filter = new RegExFileFilter(Pattern.compile(filePattern));
		File[] origFiles = path.listFiles(filter);
		Collection<File> files = new ArrayList<File>();
		for (File file : origFiles) {
			if (file.isFile()) {
				if (flags.announceObjectBankEntries) {
					System.err.println("Getting data from " + file + " (" + flags.inputEncoding + " encoding)");
				}
				files.add(file);
			}
		}

		if (files.isEmpty()) {
			throw new RuntimeException("No matching files: " + baseDir + '\t' + filePattern);
		}
		// return new ObjectBank<List<IN>>(new
		// ResettableReaderIteratorFactory(files, flags.inputEncoding),
		// readerAndWriter);
		// TODO get rid of objectbankwrapper
		return new ObjectBankWrapper<IN>(flags, new ObjectBank<List<IN>>(new ResettableReaderIteratorFactory(files,
				flags.inputEncoding), readerAndWriter), knownLCWords);
	}

	public ObjectBank<List<IN>> makeObjectBankFromFiles(Collection<File> files,
			DocumentReaderAndWriter readerAndWriter) {
		if (files.isEmpty()) {
			throw new RuntimeException("Attempt to make ObjectBank with empty file list");
		}
		// return new ObjectBank<List<IN>>(new
		// ResettableReaderIteratorFactory(files, flags.inputEncoding),
		// readerAndWriter);
		// TODO get rid of objectbankwrapper
		return new ObjectBankWrapper<IN>(flags, new ObjectBank<List<IN>>(new ResettableReaderIteratorFactory(files,
				flags.inputEncoding), readerAndWriter), knownLCWords);
	}

	/**
	 * Set up an ObjectBank that will allow one to iterate over a collection of
	 * documents obtained from the passed in Reader. Each document will be
	 * represented as a list of IN. If the ObjectBank iterator() is called until
	 * hasNext() returns false, then the Reader will be read till end of file, but
	 * no reading is done at the time of this call. Reading is done using the
	 * reading method specified in <code>flags.documentReader</code>, and for some
	 * reader choices, the column mapping given in <code>flags.map</code>.
	 *
	 * @param in
	 *          Input data addNEWLCWords do we add new lowercase words from this
	 *          data to the word shape classifier
	 * @return The list of documents
	 */
	public ObjectBank<List<IN>> makeObjectBankFromReader(BufferedReader in,
			DocumentReaderAndWriter readerAndWriter) {
		if (flags.announceObjectBankEntries) {
			System.err.println("Reading data using " + readerAndWriter.getClass());
		}
		// TODO get rid of objectbankwrapper
		// return new ObjectBank<List<IN>>(new ResettableReaderIteratorFactory(in),
		// readerAndWriter);
		return new ObjectBankWrapper<IN>(flags, new ObjectBank<List<IN>>(new ResettableReaderIteratorFactory(in),
				readerAndWriter), knownLCWords);
	}

	/**
	 * Takes the file, reads it in, and prints out the likelihood of each possible
	 * label at each point.
	 *
	 * @param filename
	 *          The path to the specified file
	 */
	public void printProbs(String filename,
			DocumentReaderAndWriter readerAndWriter) {
		// only for the OCR data does this matter
		flags.ocrTrain = false;

		ObjectBank<List<IN>> docs =
			makeObjectBankFromFile(filename, readerAndWriter);
		printProbsDocuments(docs);
	}

	/**
	 * Takes a {@link List} of documents and prints the likelihood of each
	 * possible label at each point.
	 *
	 * @param documents
	 *          A {@link List} of {@link List} of something that extends
	 *          {@link CoreMap}.
	 */
	public void printProbsDocuments(ObjectBank<List<IN>> documents) {
		for (List<IN> doc : documents) {
			printProbsDocument(doc);
			System.out.println();
		}
	}

	public abstract void printProbsDocument(List<IN> document);

	/**
	 * Load a test file, run the classifier on it, and then print the answers to
	 * stdout (with timing to stderr). This uses the value of flags.documentReader
	 * to determine testFile format.
	 *
	 * @param testFile
	 *          The file to test on.
	 */
	public void classifyAndWriteAnswers(String testFile,
			DocumentReaderAndWriter readerWriter)
	throws IOException
	{
		ObjectBank<List<IN>> documents =
			makeObjectBankFromFile(testFile, readerWriter);
		classifyAndWriteAnswers(documents, readerWriter);
	}

	public void classifyStringAndWriteAnswers(String string,
			DocumentReaderAndWriter readerWriter)
	throws IOException
	{
		ObjectBank<List<IN>> documents =
			makeObjectBankFromString(string, readerWriter);
		classifyAndWriteAnswers(documents, readerWriter);
	}

	public String[] classifyStringAndReturnAnswers(String string,
			DocumentReaderAndWriter readerWriter)
	throws IOException
	{
		ObjectBank<List<IN>> documents =
			makeObjectBankFromString(string, readerWriter);
		return classifyAndReturnAnswers(documents, readerWriter);
	}
	
	public void classifyAndWriteAnswers(String testFile, OutputStream outStream,
			DocumentReaderAndWriter readerWriter)
	throws IOException
	{
		ObjectBank<List<IN>> documents =
			makeObjectBankFromFile(testFile, readerWriter);
		classifyAndWriteAnswers(documents, outStream, readerWriter);
	}

	public void classifyAndWriteAnswers(String baseDir, String filePattern,
			DocumentReaderAndWriter readerWriter)
	throws IOException
	{
		ObjectBank<List<IN>> documents =
			makeObjectBankFromFiles(baseDir, filePattern, readerWriter);
		classifyAndWriteAnswers(documents, readerWriter);
	}

	public void classifyAndWriteAnswers(Collection<File> testFiles,
			DocumentReaderAndWriter readerWriter)
	throws IOException
	{
		ObjectBank<List<IN>> documents =
			makeObjectBankFromFiles(testFiles, readerWriter);
		classifyAndWriteAnswers(documents, readerWriter);
	}

	private void classifyAndWriteAnswers(ObjectBank<List<IN>> documents,
			DocumentReaderAndWriter readerWriter)
	throws IOException
	{
		classifyAndWriteAnswers(documents, System.out, readerWriter);
	}

	@SuppressWarnings("unchecked")
	public String[] classifyAndReturnAnswers(ObjectBank<List<IN>> documents, DocumentReaderAndWriter readerWriter){
		List<String> out = new ArrayList<String>();
		int numWords = 0;
		for (List<IN> doc : documents) {
			classify(doc);
			numWords += doc.size();
			
			List<String> answers = readerWriter.returnAnswers(doc);
			if(answers!=null){
				out.addAll(answers);
			}
		}
		String[] outArr = new String[out.size()];
		out.toArray(outArr);
		return outArr;
	}

	public void classifyAndWriteAnswers(ObjectBank<List<IN>> documents,
			OutputStream outStream,
			DocumentReaderAndWriter readerWriter)
	throws IOException
	{
		Timing timer = new Timing();

		Counter<String> entityTP = new ClassicCounter<String>();
		Counter<String> entityFP = new ClassicCounter<String>();
		Counter<String> entityFN = new ClassicCounter<String>();
		boolean resultsCounted = true;

		int numWords = 0;
		int numDocs = 0;
		for (List<IN> doc : documents) {
			classify(doc);
			numWords += doc.size();
			writeAnswers(doc, outStream, readerWriter);
			resultsCounted = (resultsCounted &&
					countResults(doc, entityTP, entityFP, entityFN));
			numDocs++;
		}
		long millis = timer.stop();
		double wordspersec = numWords / (((double) millis) / 1000);
		NumberFormat nf = new DecimalFormat("0.00"); // easier way!
		System.err.println(StringUtils.getShortClassName(this) +
				" tagged " + numWords + " words in " + numDocs +
				" documents at " + nf.format(wordspersec) +
				" words per second.");
		if (resultsCounted) {
			printResults(entityTP, entityFP, entityFN);
		}
	}

	/**
	 * Load a test file, run the classifier on it, and then print the answers to
	 * stdout (with timing to stderr). This uses the value of flags.documentReader
	 * to determine testFile format.
	 *
	 * @param testFile
	 *          The file to test on.
	 */
	public void classifyAndWriteAnswersKBest(String testFile, int k, DocumentReaderAndWriter readerAndWriter) throws Exception {
		Timing timer = new Timing();
		ObjectBank<List<IN>> documents =
			makeObjectBankFromFile(testFile, readerAndWriter);
		int numWords = 0;
		int numSentences = 0;

		for (List<IN> doc : documents) {
			Counter<List<IN>> kBest = classifyKBest(doc, AnswerAnnotation.class, k);
			numWords += doc.size();
			List<List<IN>> sorted = Counters.toSortedList(kBest);
			int n = 1;
			for (List<IN> l : sorted) {
				System.out.println("<sentence id=" + numSentences + " k=" + n + " logProb=" + kBest.getCount(l) + " prob="
						+ Math.exp(kBest.getCount(l)) + '>');
				writeAnswers(l, System.out, readerAndWriter);
				System.out.println("</sentence>");
				n++;
			}
			numSentences++;
		}

		long millis = timer.stop();
		double wordspersec = numWords / (((double) millis) / 1000);
		NumberFormat nf = new DecimalFormat("0.00"); // easier way!
		System.err.println(this.getClass().getName() + " tagged " + numWords + " words in " + numSentences
				+ " documents at " + nf.format(wordspersec) + " words per second.");
	}

	/**
	 * Load a test file, run the classifier on it, and then write a Viterbi search
	 * graph for each sequence.
	 *
	 * @param testFile
	 *          The file to test on.
	 */
	public void classifyAndWriteViterbiSearchGraph(String testFile, String searchGraphPrefix, DocumentReaderAndWriter readerAndWriter) throws Exception {
		Timing timer = new Timing();
		ObjectBank<List<IN>> documents =
			makeObjectBankFromFile(testFile, readerAndWriter);
		int numWords = 0;
		int numSentences = 0;

		for (List<IN> doc : documents) {
			DFSA<String, Integer> tagLattice = getViterbiSearchGraph(doc, AnswerAnnotation.class);
			numWords += doc.size();
			PrintWriter latticeWriter = new PrintWriter(new FileOutputStream(searchGraphPrefix + '.' + numSentences
					+ ".wlattice"));
			PrintWriter vsgWriter = new PrintWriter(new FileOutputStream(searchGraphPrefix + '.' + numSentences + ".lattice"));
			if (readerAndWriter instanceof LatticeWriter)
				((LatticeWriter) readerAndWriter).printLattice(tagLattice, doc, latticeWriter);
			tagLattice.printAttFsmFormat(vsgWriter);
			latticeWriter.close();
			vsgWriter.close();
			numSentences++;
		}

		long millis = timer.stop();
		double wordspersec = numWords / (((double) millis) / 1000);
		NumberFormat nf = new DecimalFormat("0.00"); // easier way!
		System.err.println(this.getClass().getName() + " tagged " + numWords + " words in " + numSentences
				+ " documents at " + nf.format(wordspersec) + " words per second.");
	}

	/**
	 * Write the classifications of the Sequence classifier out to stdout in a
	 * format determined by the DocumentReaderAndWriter used. If the flag
	 * <code>outputEncoding</code> is defined, the output is written in that
	 * character encoding, otherwise in the system default character encoding.
	 *
	 * @param doc
	 *          Documents to write out
	 * @param outStream
	 *          outstream to use
	 * @throws IOException
	 *           If an IO problem
	 */
	public void writeAnswers(List<IN> doc, OutputStream outStream,
			DocumentReaderAndWriter readerAndWriter)
	throws IOException
	{
		if (flags.lowerNewgeneThreshold) {
			return;
		}
		if (flags.numRuns <= 1) {
			PrintWriter out;
			if (flags.outputEncoding == null) {
				out = new PrintWriter(outStream, true);
			} else {
				out = new PrintWriter(new OutputStreamWriter(outStream, flags.outputEncoding), true);
			}
			readerAndWriter.printAnswers(doc, out);
			// out.println();
			out.flush();
		}
	}
	


	/**
	 * Count the successes and failures of the model on the given document.
	 * Fills numbers in to counters for true positives, false positives,
	 * and false negatives, and also keeps track of the entities seen.
	 * <br>
	 * Returns false if we ever encounter null for gold or guess.
	 */
	public boolean countResults(List<IN> doc, Counter<String> entityTP,
			Counter<String> entityFP,
			Counter<String> entityFN) {
		int index = 0;
		int goldIndex = 0, guessIndex = 0;
		String lastGold = "O", lastGuess = "O";

		// As we go through the document, there are two events we might be
		// interested in.  One is when a gold entity ends, and the other
		// is when a guessed entity ends.  If the gold and guessed
		// entities end at the same time, started at the same time, and
		// match entity type, we have a true positive.  Otherwise we
		// either have a false positive or a false negative.
		for (IN line : doc) {
			String gold = line.get(GoldAnswerAnnotation.class);
			String guess = line.get(AnswerAnnotation.class);

			if (gold == null || guess == null)
				return false;

			if (!lastGold.equals(gold) && !lastGold.equals("O")) {
				if (lastGuess.equals(lastGold) && !lastGuess.equals(guess) &&
						goldIndex == guessIndex) {
					entityTP.incrementCount(lastGold, 1.0);
				} else {
					entityFN.incrementCount(lastGold, 1.0);
				}
			}

			if (!lastGuess.equals(guess) && !lastGuess.equals("O")) {
				if (lastGuess.equals(lastGold) && !lastGuess.equals(guess) &&
						goldIndex == guessIndex && !lastGold.equals(gold)) {
					// correct guesses already tallied
					// only need to tally false positives
				} else {
					entityFP.incrementCount(lastGuess, 1.0);
				}
			}

			if (!lastGold.equals(gold)) {
				lastGold = gold;
				goldIndex = index;
			}

			if (!lastGuess.equals(guess)) {
				lastGuess = guess;
				guessIndex = index;
			}
			++index;
		}

		// We also have to account for entities at the very end of the
		// document, since the above logic only occurs when we see
		// something that tells us an entity has ended
		if (!lastGold.equals("O")) {
			if (lastGold.equals(lastGuess) && goldIndex == guessIndex) {
				entityTP.incrementCount(lastGold, 1.0);
			} else {
				entityFN.incrementCount(lastGold, 1.0);
			}
		}
		if (!lastGuess.equals("O")) {
			if (lastGold.equals(lastGuess) && goldIndex == guessIndex) {
				// correct guesses already tallied
			} else {
				entityFP.incrementCount(lastGuess, 1.0);
			}
		}
		return true;
	}

	/**
	 * Given counters of true positives, false positives, and false
	 * negatives, prints out precision, recall, and f1 for each key.
	 */
	public void printResults(Counter<String> entityTP, Counter<String> entityFP,
			Counter<String> entityFN) {
		Set<String> entities = new TreeSet<String>();
		entities.addAll(entityTP.keySet());
		entities.addAll(entityFP.keySet());
		entities.addAll(entityFN.keySet());
		boolean printedHeader = false;
		for (String entity : entities) {
			double tp = entityTP.getCount(entity);
			double fp = entityFP.getCount(entity);
			double fn = entityFN.getCount(entity);
			if (tp == 0.0 && (fp == 0.0 || fn == 0.0))
				continue;
			double precision = tp / (tp + fp);
			double recall = tp / (tp + fn);
			double f1 = ((precision == 0.0 || recall == 0.0) ?
					0.0 : 2.0 / (1.0 / precision + 1.0 / recall));
			if (!printedHeader) {
				System.err.println("         Entity\tP\tR\tF1\tTP\tFP\tFN");
				printedHeader = true;
			}
			System.err.format("%15s\t%.4f\t%.4f\t%.4f\t%.0f\t%.0f\t%.0f\n",
					entity, precision, recall, f1,
					tp, fp, fn);
		}
	}

	/**
	 * Serialize a sequence classifier to a file on the given path.
	 *
	 * @param serializePath
	 *          The path/filename to write the classifier to.
	 */
	public abstract void serializeClassifier(String serializePath);

	/**
	 * Loads a classifier from the given input stream. The JVM shuts down
	 * (System.exit(1)) if there is an exception. This does not close the
	 * InputStream.
	 *
	 * @param in
	 *          The InputStream to read from
	 */
	public void loadClassifierNoExceptions(InputStream in, Properties props) {
		// load the classifier
		try {
			loadClassifier(in, props);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Load a classifier from the specified InputStream. No extra properties are
	 * supplied. This does not close the InputStream.
	 *
	 * @param in
	 *          The InputStream to load the serialized classifier from
	 *
	 * @throws IOException
	 *           If there are problems accessing the input stream
	 * @throws ClassCastException
	 *           If there are problems interpreting the serialized data
	 * @throws ClassNotFoundException
	 *           If there are problems interpreting the serialized data
	 */
	public void loadClassifier(InputStream in) throws IOException, ClassCastException, ClassNotFoundException {
		loadClassifier(in, null);
	}

	/**
	 * Load a classifier from the specified InputStream. The classifier is
	 * reinitialized from the flags serialized in the classifier. This does not
	 * close the InputStream.
	 *
	 * @param in
	 *          The InputStream to load the serialized classifier from
	 * @param props
	 *          This Properties object will be used to update the
	 *          SeqClassifierFlags which are read from the serialized classifier
	 *
	 * @throws IOException
	 *           If there are problems accessing the input stream
	 * @throws ClassCastException
	 *           If there are problems interpreting the serialized data
	 * @throws ClassNotFoundException
	 *           If there are problems interpreting the serialized data
	 */
	public void loadClassifier(InputStream in, Properties props) throws IOException, ClassCastException,
	ClassNotFoundException {
		loadClassifier(new ObjectInputStream(in), props);
	}

	/**
	 * Load a classifier from the specified input stream. The classifier is
	 * reinitialized from the flags serialized in the classifier.
	 *
	 * @param in
	 *          The InputStream to load the serialized classifier from
	 * @param props
	 *          This Properties object will be used to update the
	 *          SeqClassifierFlags which are read from the serialized classifier
	 *
	 * @throws IOException
	 *           If there are problems accessing the input stream
	 * @throws ClassCastException
	 *           If there are problems interpreting the serialized data
	 * @throws ClassNotFoundException
	 *           If there are problems interpreting the serialized data
	 */
	public abstract void loadClassifier(ObjectInputStream in, Properties props) throws IOException, ClassCastException,
	ClassNotFoundException;

	private InputStream loadStreamFromClasspath(String path) {
		InputStream is = getClass().getClassLoader().getResourceAsStream(path);
		if (is == null)
			return null;
		try {
			if (path.endsWith(".gz"))
				is = new GZIPInputStream(new BufferedInputStream(is));
			else
				is = new BufferedInputStream(is);
		} catch (IOException e) {
			System.err.println("CLASSPATH resource " + path + " is not a GZIP stream!");
		}
		return is;
	}

	/**
	 * Loads a classifier from the file specified by loadPath. If loadPath ends in
	 * .gz, uses a GZIPInputStream, else uses a regular FileInputStream.
	 */
	public void loadClassifier(String loadPath) throws ClassCastException, IOException, ClassNotFoundException {
		loadClassifier(loadPath, null);
	}

	/**
	 * Loads a classifier from the file specified by loadPath. If loadPath ends in
	 * .gz, uses a GZIPInputStream, else uses a regular FileInputStream.
	 */
	public void loadClassifier(String loadPath, Properties props) throws ClassCastException, IOException, ClassNotFoundException {
		InputStream is;
		// ms, 10-04-2010: check first is this path exists in our CLASSPATH. This
		// takes priority over the file system.
		if ((is = loadStreamFromClasspath(loadPath)) != null) {
			Timing.startDoing("Loading classifier from " + loadPath);
			loadClassifier(is);
			is.close();
			Timing.endDoing();
		} else {
			loadClassifier(new File(loadPath), props);
		}
	}

	public void loadClassifierNoExceptions(String loadPath) {
		loadClassifierNoExceptions(loadPath, null);
	}

	public void loadClassifierNoExceptions(String loadPath, Properties props) {
		InputStream is;
		// ms, 10-04-2010: check first is this path exists in our CLASSPATH. This
		// takes priority over the file system.
		if ((is = loadStreamFromClasspath(loadPath)) != null) {
			Timing.startDoing("Loading classifier from " + loadPath);
			loadClassifierNoExceptions(is, props);
			try {
				is.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			Timing.endDoing();
		} else {
			loadClassifierNoExceptions(new File(loadPath), props);
		}
	}

	public void loadClassifier(File file) throws ClassCastException, IOException, ClassNotFoundException {
		loadClassifier(file, null);
	}

	/**
	 * Loads a classifier from the file specified. If the file's name ends in .gz,
	 * uses a GZIPInputStream, else uses a regular FileInputStream. This method
	 * closes the File when done.
	 *
	 * @param file
	 *          Loads a classifier from this file.
	 * @param props
	 *          Properties in this object will be used to overwrite those
	 *          specified in the serialized classifier
	 *
	 * @throws IOException
	 *           If there are problems accessing the input stream
	 * @throws ClassCastException
	 *           If there are problems interpreting the serialized data
	 * @throws ClassNotFoundException
	 *           If there are problems interpreting the serialized data
	 */
	public void loadClassifier(File file, Properties props) throws ClassCastException, IOException,
	ClassNotFoundException {
		Timing.startDoing("Loading classifier from " + file.getAbsolutePath());
		BufferedInputStream bis;
		if (file.getName().endsWith(".gz")) {
			bis = new BufferedInputStream(new GZIPInputStream(new FileInputStream(file)));
		} else {
			bis = new BufferedInputStream(new FileInputStream(file));
		}
		loadClassifier(bis, props);
		bis.close();
		Timing.endDoing();
	}

	public void loadClassifierNoExceptions(File file) {
		loadClassifierNoExceptions(file, null);
	}

	public void loadClassifierNoExceptions(File file, Properties props) {
		try {
			loadClassifier(file, props);
		} catch (Exception e) {
			System.err.println("Error deserializing " + file.getAbsolutePath());
			throw new RuntimeException(e);
		}
	}

	/**
	 * This function will load a classifier that is stored inside a jar file (if
	 * it is so stored). The classifier should be specified as its full filename,
	 * but the path in the jar file (<code>/classifiers/</code>) is coded in this
	 * class. If the classifier is not stored in the jar file or this is not run
	 * from inside a jar file, then this function will throw a RuntimeException.
	 *
	 * @param modelName
	 *          The name of the model file. Iff it ends in .gz, then it is assumed
	 *          to be gzip compressed.
	 * @param props
	 *          A Properties object which can override certain properties in the
	 *          serialized file, such as the DocumentReaderAndWriter. You can pass
	 *          in <code>null</code> to override nothing.
	 */
	public void loadJarClassifier(String modelName, Properties props) {
		Timing.startDoing("Loading JAR-internal classifier " + modelName);
		try {
			InputStream is = getClass().getResourceAsStream(modelName);
			if (modelName.endsWith(".gz")) {
				is = new GZIPInputStream(is);
			}
			is = new BufferedInputStream(is);
			loadClassifier(is, props);
			is.close();
			Timing.endDoing();
		} catch (Exception e) {
			String msg = "Error loading classifier from jar file (most likely you are not running this code from a jar file or the named classifier is not stored in the jar file)";
			throw new RuntimeException(msg, e);
		}
	}

	private transient PrintWriter cliqueWriter;
	private transient int writtenNum; // = 0;

	/** Print the String features generated from a IN */
	protected void printFeatures(IN wi, Collection<String> features) {
		if (flags.printFeatures == null || writtenNum > flags.printFeaturesUpto) {
			return;
		}
		try {
			if (cliqueWriter == null) {
				cliqueWriter = new PrintWriter(new FileOutputStream("feats" + flags.printFeatures + ".txt"), true);
				writtenNum = 0;
			}
		} catch (Exception ioe) {
			throw new RuntimeException(ioe);
		}
		if (writtenNum >= flags.printFeaturesUpto) {
			return;
		}
		if (wi instanceof CoreLabel) {
			cliqueWriter.print(wi.get(TextAnnotation.class) + ' ' + wi.get(PartOfSpeechAnnotation.class) + ' '
					+ wi.get(CoreAnnotations.GoldAnswerAnnotation.class) + '\t');
		} else {
			cliqueWriter.print(wi.get(CoreAnnotations.TextAnnotation.class)
					+ wi.get(CoreAnnotations.GoldAnswerAnnotation.class) + '\t');
		}
		boolean first = true;
		for (Object feat : features) {
			if (first) {
				first = false;
			} else {
				cliqueWriter.print(" ");
			}
			cliqueWriter.print(feat);
		}
		cliqueWriter.println();
		writtenNum++;
	}

	/** Print the String features generated from a token */
	protected void printFeatureLists(IN wi, Collection<List<String>> features) {
		if (flags.printFeatures == null || writtenNum > flags.printFeaturesUpto) {
			return;
		}
		try {
			if (cliqueWriter == null) {
				cliqueWriter = new PrintWriter(new FileOutputStream("feats" + flags.printFeatures + ".txt"), true);
				writtenNum = 0;
			}
		} catch (Exception ioe) {
			throw new RuntimeException(ioe);
		}
		if (writtenNum >= flags.printFeaturesUpto) {
			return;
		}
		if (wi instanceof CoreLabel) {
			cliqueWriter.print(wi.get(TextAnnotation.class) + ' ' + wi.get(PartOfSpeechAnnotation.class) + ' '
					+ wi.get(CoreAnnotations.GoldAnswerAnnotation.class) + '\t');
		} else {
			cliqueWriter.print(wi.get(CoreAnnotations.TextAnnotation.class)
					+ wi.get(CoreAnnotations.GoldAnswerAnnotation.class) + '\t');
		}
		boolean first = true;
		for (List<String> featList : features) {
			for (String feat : featList) {
				if (first) {
					first = false;
				} else {
					cliqueWriter.print(" ");
				}
				cliqueWriter.print(feat);
			}
			cliqueWriter.print("  ");
		}
		cliqueWriter.println();
		writtenNum++;
	}

}
