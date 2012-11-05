package edu.stanford.nlp.trees;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * A base class for Head Finders similar to the one described in
 * Michael Collins' 1999 thesis.  For a given constituent we perform
 * <p/>
 * for categoryList in categoryLists
 * for index = 1 to n [or n to 1 if R->L]
 * for category in categoryList
 * if category equals daughter[index] choose it.
 * <p/>
 * with a final default that goes with the direction (L->R or R->L)
 * For most constituents, there will be only one category in the list,
 * the exception being, in Collins' original version, NP.
 * <p/>
 * It is up to the overriding base class to initialize the map
 * from constituent type to categoryLists, "nonTerminalInfo",
 * in its constructor.
 * Entries are presumed to be of type String[][].  Each String[] is a list of
 * categories, except for the first entry, which specifies direction of
 * traversal and must be one of "right", "left" or "rightdis" or "leftdis".
 * <p/>
 * "left" means search left-to-right by category and then by position
 * "leftdis" means search left-to-right by position and then by category
 * "right" means search right-to-left by category and then by position
 * "rightdis" means search right-to-left by position and then by category
 * "leftexcept" means to take the first thing from the left that isn't in the list
 * "rightexcept" means to take the first thing from the right that isn't on the list
 * <p/>
 * <p/>
 * 2002/10/28 -- Category label identity checking now uses the
 * equals() method instead of ==, so not interning category labels
 * shouldn't break things anymore.  (Roger Levy) <br>
 * 2003/02/10 -- Changed to use TreebankLanguagePack and to cut on
 * characters that set off annotations, so this should work even if
 * functional tags are still on nodes. <br>
 * 2004/03/30 -- Made abstract base class and subclasses for CollinsHeadFinder,
 * ModCollinsHeadFinder, SemanticHeadFinder, ChineseHeadFinder
 * (and trees.icegb.ICEGBHeadFinder, trees.international.negra.NegraHeadFinder,
 * and movetrees.EnglishPennMaxProjectionHeadFinder)
 * 2011/01/13 -- Add support for categoriesToAvoid (which can be set to ensure that
 * punctuation is not the head if there are other options)
 *
 * @author Christopher Manning
 * @author Galen Andrew
 */
public abstract class AbstractCollinsHeadFinder implements HeadFinder /* Serializable */ {

  private static final boolean DEBUG = false;
  protected final TreebankLanguagePack tlp;
  protected Map<String, String[][]> nonTerminalInfo;

  /** Default direction if no rule is found for category.
   *  Subclasses can turn it on if they like.
   *  If they don't it is an error if no rule is defined for a category
   *  (null is returned).
   */
  protected String[] defaultRule; // = null;
  
  /** These are built automatically from categoriesToAvoid and used in a fairly
   *  different fashion from defaultRule (above).  These are used for categories
   *  that do have defined rules but where none of them have matched.  Rather
   *  than picking the rightmost or leftmost child, we will use these to pick
   *  the the rightmost or leftmost child which isn't in categoriesToAvoid.
   */
  private String[] defaultLeftRule;
  private String[] defaultRightRule;

  protected AbstractCollinsHeadFinder(TreebankLanguagePack tlp) {
    this.tlp = tlp;
    setCategoriesToAvoid(new String[] {});
  }

  /**
   * Set categories which, if it comes to last resort processing (i.e. none of
   * the rules matched), will be avoided as heads. In last resort processing,
   * it will attempt to match the leftmost or rightmost constituent not in this
   * set but will fall back to the left or rightmost constituent if necessary.
   * 
   * @param categoriesToAvoid list of constituent types to avoid
   */
  protected void setCategoriesToAvoid(String[] categoriesToAvoid) {
    // automatically build defaultLeftRule, defaultRightRule
    ArrayList<String> asList = new ArrayList<String>(Arrays.asList(categoriesToAvoid));
    asList.add(0, "leftexcept");
    defaultLeftRule = new String[asList.size()];
    defaultRightRule = new String[asList.size()];
    asList.toArray(defaultLeftRule);
    asList.set(0, "rightexcept");
    asList.toArray(defaultRightRule);
  }

  /**
   * A way for subclasses for corpora with explicit head markings
   * to return the explicitly marked head
   *
   * @param t a tree to find the head of
   * @return the marked head-- null if no marked head
   */
  // to be overridden in subclasses for corpora
  //
  protected Tree findMarkedHead(Tree t) {
    return null;
  }

  /**
   * Determine which daughter of the current parse tree is the head.
   *
   * @param t The parse tree to examine the daughters of.
   *          If this is a leaf, <code>null</code> is returned
   * @return The daughter parse tree that is the head of <code>t</code>
   * @see Tree#percolateHeads(HeadFinder)
   *      for a routine to call this and spread heads throughout a tree
   */
  public Tree determineHead(Tree t) {
    return determineHead(t, null);
  }

  /**
   * Determine which daughter of the current parse tree is the head.
   *
   * @param t The parse tree to examine the daughters of.
   *          If this is a leaf, <code>null</code> is returned
   * @param parent The parent of t
   * @return The daughter parse tree that is the head of <code>t</code>.
   *   Returns null for leaf nodes.
   * @see Tree#percolateHeads(HeadFinder)
   *      for a routine to call this and spread heads throughout a tree
   */
  public Tree determineHead(Tree t, Tree parent) {
    if (nonTerminalInfo == null) {
      throw new RuntimeException("Classes derived from AbstractCollinsHeadFinder must" + " create and fill HashMap nonTerminalInfo.");
    }
    if (DEBUG) {
      System.err.println("determineHead for " + t.value());
    }

    if (t.isLeaf()) {
      return null;
    }
    Tree[] kids = t.children();

    Tree theHead;
    // first check if subclass found explicitly marked head
    if ((theHead = findMarkedHead(t)) != null) {
      if (DEBUG) {
        System.err.println("Find marked head method returned " +
                           theHead.label() + " as head of " + t.label());
      }
      return theHead;
    }

    // if the node is a unary, then that kid must be the head
    // it used to special case preterminal and ROOT/TOP case
    // but that seemed bad (especially hardcoding string "ROOT")
    if (kids.length == 1) {
      if (DEBUG) {
        System.err.println("Only one child determines " +
                           kids[0].label() + " as head of " + t.label());
      }
      return kids[0];
    }

    return determineNonTrivialHead(t, parent);
  }

  /** Called by determineHead and may be overridden in subclasses
   *  if special treatment is necessary for particular categories.
   */
  protected Tree determineNonTrivialHead(Tree t, Tree parent) {
    Tree theHead = null;
    String motherCat = tlp.basicCategory(t.label().value());
    if (DEBUG) {
      System.err.println("Looking for head of " + t.label() +
                         "; value is |" + t.label().value() + "|, " +
                         " baseCat is |" + motherCat + '|');
    }
    // We know we have nonterminals underneath
    // (a bit of a Penn Treebank assumption, but).

    // Look at label.
    // a total special case....
    // first look for POS tag at end
    // this appears to be redundant in the Collins case since the rule already would do that
    //    Tree lastDtr = t.lastChild();
    //    if (tlp.basicCategory(lastDtr.label().value()).equals("POS")) {
    //      theHead = lastDtr;
    //    } else {
    String[][] how = nonTerminalInfo.get(motherCat);
    if (how == null) {
      if (DEBUG) {
        System.err.println("Warning: No rule found for " + motherCat +
                           " (first char: " + motherCat.charAt(0) + ')');
        System.err.println("Known nonterms are: " + nonTerminalInfo.keySet());
      }
      if (defaultRule != null) {
        if (DEBUG) {
          System.err.println("  Using defaultRule");
        }
        return traverseLocate(t.children(), defaultRule, true);
      } else {
        return null;
      }
    }
    for (int i = 0; i < how.length; i++) {
      boolean lastResort = (i == how.length - 1);
      theHead = traverseLocate(t.children(), how[i], lastResort);
      if (theHead != null) {
        break;
      }
    }
    if (DEBUG) {
      System.err.println("  Chose " + theHead.label());
    }
    return theHead;
  }

  /**
   * Attempt to locate head daughter tree from among daughters.
   * Go through daughterTrees looking for things from a set found by
   * looking up the motherkey specifier in a hash map, and if
   * you do not find one, take leftmost or rightmost thing iff
   * lastResort is true, otherwise return <code>null</code>.
   */
  protected Tree traverseLocate(Tree[] daughterTrees, String[] how, boolean lastResort) {
    int headIdx = 0;
    String childCat;
    boolean found = false;

    if (how[0].equals("left")) {
      twoloop:
        for (int i = 1; i < how.length; i++) {
          for (headIdx = 0; headIdx < daughterTrees.length; headIdx++) {
            childCat = tlp.basicCategory(daughterTrees[headIdx].label().value());
            if (how[i].equals(childCat)) {
              found = true;
              break twoloop;
            }
          }
        }
    } else if (how[0].equals("leftdis")) {
      twoloop:
        for (headIdx = 0; headIdx < daughterTrees.length; headIdx++) {
          childCat = tlp.basicCategory(daughterTrees[headIdx].label().value());
          for (int i = 1; i < how.length; i++) {
            if (how[i].equals(childCat)) {
              found = true;
              break twoloop;
            }
          }
        }
    } else if (how[0].equals("right")) {
      // from right
      twoloop:
        for (int i = 1; i < how.length; i++) {
          for (headIdx = daughterTrees.length - 1; headIdx >= 0; headIdx--) {
            childCat = tlp.basicCategory(daughterTrees[headIdx].label().value());
            if (how[i].equals(childCat)) {
              found = true;
              break twoloop;
            }
          }
        }
    } else if (how[0].equals("rightdis")) {
      // from right, but search for any, not in turn
      twoloop:
        for (headIdx = daughterTrees.length - 1; headIdx >= 0; headIdx--) {
          childCat = tlp.basicCategory(daughterTrees[headIdx].label().value());
          for (int i = 1; i < how.length; i++) {
            if (how[i].equals(childCat)) {
              found = true;
              break twoloop;
            }
          }
        }
    } else if (how[0].equals("leftexcept")) {
      for (headIdx = 0; headIdx < daughterTrees.length; headIdx++) {
        childCat = tlp.basicCategory(daughterTrees[headIdx].label().value());
        found = true;
        for (int i = 1; i < how.length; i++) {
          if (how[i].equals(childCat)) {
            found = false;
          }
        }
        if (found) {
          break;
        }
      }
    } else if (how[0].equals("rightexcept")) {
      for (headIdx = daughterTrees.length - 1; headIdx >= 0; headIdx--) {
        childCat = tlp.basicCategory(daughterTrees[headIdx].label().value());
        found = true;
        for (int i = 1; i < how.length; i++) {
          if (how[i].equals(childCat)) {
            found = false;
          }
        }
        if (found) {
          break;
        }
      }
    } else {
      throw new RuntimeException("ERROR: invalid direction type " + how[0] + " to nonTerminalInfo map in AbstractCollinsHeadFinder.");
    }
    
    // what happens if our rule didn't match anything
    if (!found) {
      if (lastResort) {
        // use the default rule to try to match anything except categoriesToAvoid
        // if that doesn't match, we'll return the left or rightmost child (by
        // setting headIdx).  We want to be careful to ensure that postOperationFix
        // runs exactly once.
        String[] rule;
        if (how[0].startsWith("left")) {
          headIdx = 0;
          rule = defaultLeftRule;
        } else {
          headIdx = daughterTrees.length - 1;
          rule = defaultRightRule;
        }
        Tree child = traverseLocate(daughterTrees, rule, false);
        if (child != null) {
          return child;
        }
      } else {
        // if we're not the last resort, we can return null to let the next rule try to match
        return null;
      }
    }
    
    headIdx = postOperationFix(headIdx, daughterTrees);

    return daughterTrees[headIdx];
  }

  /**
   * A way for subclasses to fix any heads under special conditions
   * The default does nothing.
   *
   * @param headIdx       the index of the proposed head
   * @param daughterTrees the array of daughter trees
   * @return the new headIndex
   */
  protected int postOperationFix(int headIdx, Tree[] daughterTrees) {
    return headIdx;
  }

  private static final long serialVersionUID = -6540278059442931087L;

}
