package ivory.bloomir.data;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import ivory.bloomir.util.DocumentUtility;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.core.data.stat.SpamPercentileScore;

public class CompressedPostingsIO {
  private static final Logger LOGGER = Logger.getLogger(CompressedPostingsIO.class);
  public static final String LENGTH_FILE = "length";

  /**
   * Reads the number of terms from an index
   *
   * @param path Path to the root of the postings list
   * @param fs File system
   * @return Number of terms in the index (i.e., number of postings lists)
   */
  public static int readNumberOfTerms(String path, FileSystem fs)
    throws IOException, ClassNotFoundException {
    FSDataInputStream input = fs.open(new Path(path + "/" + LENGTH_FILE));
    int numberOfTerms = input.readInt();
    input.close();
    return numberOfTerms;
  }

  /**
   * Loads an entire collection of postings lists and initializes
   * the given ranker with this collection.
   *
   * @param path Path to the root of the postings list
   * @param fs File system
   * @param postings Array of {@link CompressedPostings} to initialize
   * @param dfs Array of integers to be initialized (this represents Document Frequencies)
   */
  public static void loadPostings(String path, FileSystem fs, CompressedPostings[] postings, int[] dfs)
    throws IOException, ClassNotFoundException {
    FSDataInputStream input;
    FileStatus[] stat = fs.listStatus(new Path(path));
    for(int f = 0; f < stat.length; f++) {
      String name = stat[f].getPath().toString();
      name = name.substring(name.lastIndexOf('/') + 1);

      if(name.equals(LENGTH_FILE)) {
        continue;
      }

      LOGGER.info("reading block: " + name);
      input = fs.open(stat[f].getPath());

      while(true) {
        try {
          int id = input.readInt();
          dfs[id] = input.readInt();
          postings[id] = CompressedPostings.readInstance(input);
        } catch(EOFException ex) {
          break;
        }
      }
      input.close();
    }
  }

  /**
   * Converts the postings of a collection into CompressedPostings and writes
   * them to disk.
   *
   * @param outputPath Root path to store the output in
   * @param fs File system
   * @param env A retrieval environment
   * @param spamScoresPath Path to spam/quality scores
   */
  public static void writePostings(String outputPath, FileSystem fs, RetrievalEnvironment env, String spamScoresPath)
    throws IOException {
    SpamPercentileScore spamScores = new SpamPercentileScore();
    spamScores.initialize(spamScoresPath, fs);
    int[] newDocids = DocumentUtility.spamSortDocids(spamScores);

    int collectionSize = env.readCollectionTermCount();
    Posting posting = new Posting();
    FSDataOutputStream out;

    out = fs.create(new Path(outputPath + "/" + CompressedPostingsIO.LENGTH_FILE));
    out.writeInt(collectionSize);
    out.close();

    for(int i = 0; i <= collectionSize; i++) {
      if(i % 100000 == 0) {
        if(i != 0) {
          out.close();
        }
        out = fs.create(new Path(outputPath + "/" + i));
      }

      if(i % 1000 == 0) {
        LOGGER.info(i + " posting lists prepared...");
      }

      try {
        PostingsList pl = env.getPostingsList(env.getTermFromId(i));
        PostingsReader reader = pl.getPostingsReader();

        int[] data = new int[pl.getDf()];
        int index = 0;
        while (reader.nextPosting(posting)) {
          data[index++] = newDocids[posting.getDocno()];
        }
        Arrays.sort(data);
        CompressedPostings compPostings = CompressedPostings.newInstance(data);

        out.writeInt(i);
        out.writeInt(pl.getDf());
        compPostings.write(out);
      } catch(Exception e) {
        continue;
      }
    }

    if(out != null) {
      out.close();
    }
  }
}
