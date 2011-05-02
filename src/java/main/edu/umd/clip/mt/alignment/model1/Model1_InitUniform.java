package edu.umd.clip.mt.alignment.model1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.clip.mt.Alignment;
import edu.umd.clip.mt.AlignmentPosteriorGrid;
import edu.umd.clip.mt.PhrasePair;
import edu.umd.clip.mt.alignment.CrossEntropyCounters;
import edu.umd.clip.mt.alignment.PerplexityReporter;

public class Model1_InitUniform extends Model1Base {

	IntWritable nullWord = new IntWritable(0);
	public Model1_InitUniform(boolean useNullWord)
	{ super(useNullWord); }
	
	@Override
	public void processTrainingInstance(PhrasePair pp, Reporter reporter) {
		initializeCountTableForSentencePair(pp);
		int f_len = pp.getF().size();
		int e_len = pp.getE().size();
		
		int startI = 1;
		if (_includeEnglishNullWord) startI = 0;
	
		for (int i=startI; i<=e_len; i++) {
			for (int j=0; j<f_len; j++) {
				addTranslationCount(i, j, 1.0f);
			}
		}
		if (reporter != null) {
			reporter.incrCounter(CrossEntropyCounters.LOGPROB, 0);
			reporter.incrCounter(CrossEntropyCounters.WORDCOUNT, f_len);
			reporter.progress();
		}
	}

	@Override
	public void clearModel() {}

	@Override
	public Alignment viterbiAlign(PhrasePair pp, PerplexityReporter r) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public AlignmentPosteriorGrid computeAlignmentPosteriors(PhrasePair pp) {
		// TODO Auto-generated method stub
		return null;
	}
}
