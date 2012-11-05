package edu.stanford.nlp.trees;

import edu.stanford.nlp.process.Morphology;


/**
 * Stems the Words in a Tree using Morphology.
 *
 * @author Huy Nguyen (htnguyen@cs.stanford.edu)
 */
public class WordStemmer implements TreeVisitor {

  private final Morphology morpha;

  public WordStemmer() {
    morpha = new Morphology();
  }

  public void visitTree(Tree t) {
    processTree(t, null);
  }

  private void processTree(Tree t, String tag) {
    if (t.isPreTerminal()) {
      tag = t.label().value();
    }
    if (t.isLeaf()) {
      t.label().setValue(morpha.lemma(t.label().value(), tag));
    } else {
      for (Tree kid : t.children()) {
        processTree(kid, tag);
      }
    }
  }

  /**
   * Reads, stems, and prints the trees in the file.
   *
   * @param args Usage: WordStemmer file
   */
  public static void main(String[] args) {
    Treebank treebank = new DiskTreebank();
    treebank.loadPath(args[0]);
    WordStemmer ls = new WordStemmer();
    for (Tree tree : treebank) {
      ls.visitTree(tree);
      System.out.println(tree);
    }
  }

}
