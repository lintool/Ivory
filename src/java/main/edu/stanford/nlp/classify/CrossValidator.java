package edu.stanford.nlp.classify;

import edu.stanford.nlp.util.Pair;
import edu.stanford.nlp.util.Triple;
import edu.stanford.nlp.util.Function;

import java.util.Iterator;

/**
 * This class is meant to simplify performing cross validation on
 * classifiers for hyper-parameters.  It has the ability to save
 * state for each fold (for instance, the weights for a MaxEnt
 * classifier, and the alphas for an SVM).
 *
 * @author Aria Haghighi
 * @author Jenny Finkel
 * @author Sarah Spikes (Templatization)
 */

public class CrossValidator<L, F> {
  private GeneralDataset<L, F> originalTrainData;
  private int kfold;
  private SavedState[] savedStates;

  public CrossValidator(GeneralDataset<L, F> trainData) {
    this (trainData,5);
  }

  public CrossValidator(GeneralDataset<L, F> trainData, int kfold) {
    originalTrainData = trainData;
    this.kfold = kfold;
    savedStates = new SavedState[kfold];
    for (int i = 0; i < savedStates.length; i++) {
      savedStates[i] = new SavedState();
    }
  }

  /**
   * Returns and Iterator over train/test/saved states
   */
  private Iterator<Triple<GeneralDataset<L, F>,GeneralDataset<L, F>,SavedState>> iterator() { return new CrossValidationIterator(); }

  /**
   * This computes the average over all folds of the function we're trying to optimize.
   * The input triple contains, in order, the train set, the test set, and the saved state.  
   * You don't have to use the saved state if you don't want to.
   */
  public double computeAverage (Function<Triple<GeneralDataset<L, F>,GeneralDataset<L, F>,SavedState>,Double> function) 
  {
    double sum = 0;
    Iterator<Triple<GeneralDataset<L, F>,GeneralDataset<L, F>,SavedState>> foldIt = iterator();
    while (foldIt.hasNext()) {
      sum += function.apply(foldIt.next());
    }
    return sum / kfold;
  }

  class CrossValidationIterator implements Iterator<Triple<GeneralDataset<L, F>,GeneralDataset<L, F>,SavedState>>
  {
    int iter = 0;
    public boolean hasNext() { return iter < kfold; }

    public void remove()
    {
      throw new RuntimeException("CrossValidationIterator doesn't support remove()");
    }
  
    public Triple<GeneralDataset<L, F>,GeneralDataset<L, F>,SavedState> next()
    {
      if (iter == kfold) return null;
      int start = originalTrainData.size() * iter / kfold;
      int end = originalTrainData.size() * (iter + 1) / kfold;
      //System.err.println("##train data size: " +  originalTrainData.size() + " start " + start + " end " + end);
      Pair<GeneralDataset<L, F>, GeneralDataset<L, F>> split = originalTrainData.split(start, end);
      
      return new Triple<GeneralDataset<L, F>,GeneralDataset<L, F>,SavedState>(split.first(),split.second(),savedStates[iter++]);
    }
  }
  
  public static class SavedState {
    public Object state;
  }

  public static void main(String[] args) {
    Dataset<String, String> d = Dataset.readSVMLightFormat(args[0]);
    Iterator<Triple<GeneralDataset<String, String>,GeneralDataset<String, String>,SavedState>> it = (new CrossValidator<String, String>(d)).iterator();
    while (it.hasNext()) 
    { 
      it.next(); 
      break;
    }
  }
}
