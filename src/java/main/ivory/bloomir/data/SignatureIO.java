package ivory.bloomir.data;

import java.io.EOFException;
import java.io.IOException;

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

public class SignatureIO {
  private static final Logger LOGGER = Logger.getLogger(SignatureIO.class);

  /**
   * Loads an entire collection of signatures into an array.
   *
   * @param path Path to the root of the postings list
   * @param fs File system
   * @param signatures Array of {@link Signature} to be initialized.
   */
  public static void loadSignatures(String path, FileSystem fs, Signature[] signatures) throws IOException {
    FSDataInputStream input = fs.open(new Path(path + "/" + BloomConfig.CONFIG_FILE));
    BloomConfig bloomConfig = BloomConfig.readInstance(input);
    input.close();

    FileStatus[] stat = fs.listStatus(new Path(path));
    for(int f = 0; f < stat.length; f++) {
      String name = stat[f].getPath().toString();
      name = name.substring(name.lastIndexOf('/') + 1);

      if(name.equals(BloomConfig.CONFIG_FILE)) {
        continue;
      }

      LOGGER.info("reading block: " + name);
      input = fs.open(stat[f].getPath());

      while(true) {
        try {
          int id = input.readInt();
          int df = input.readInt();
          if(df <= bloomConfig.getIdentityHashThreshold()) {
            signatures[id] = BloomFilterHash.readInstance(input);
          } else {
            signatures[id] = BloomFilterIdentityHash.readInstance(input);
          }
        } catch(EOFException ex) {
          break;
        }
      }
      input.close();
    }
  }

  /**
   * Creates Signatures and writes them to disk.
   *
   * @param outputPath Root path to store the output in.
   * @param fs File system
   * @param env Retireval Environment
   * @param spamScoresPath Path to spam/quality scores
   * @param bitsPerElement Number of bits dedicated to one element
   * @param nbHash Number of hash functions
   */
  public static void writeSignatures(String outputPath, FileSystem fs, RetrievalEnvironment env,
                                     String spamScoresPath, int bitsPerElement, int nbHash) throws IOException {
    SpamPercentileScore spamScores = new SpamPercentileScore();
    spamScores.initialize(spamScoresPath, fs);
    int[] newDocids = DocumentUtility.spamSortDocids(spamScores);

    int collectionSize = env.readCollectionTermCount();
    Posting posting = new Posting();
    FSDataOutputStream out;

    BloomConfig bloomConfig =  new BloomConfig((int) env.getDocumentCount(),
                                               collectionSize, nbHash, bitsPerElement);
    //Deletes the output path if it already exists.
    fs.delete(new Path(outputPath));

    //Serialize and write the configuration parameters.
    out = fs.create(new Path(outputPath + "/" + BloomConfig.CONFIG_FILE));
    bloomConfig.write(out);
    out.close();

    for(int i = 0; i <= collectionSize; i++) {
      if(i % 100000 == 0) {
        if(i != 0) {
          out.close();
        }
        out = fs.create(new Path(outputPath + "/" + i));
      }

      try {
        PostingsList pl = env.getPostingsList(env.getTermFromId(i));
        PostingsReader reader = pl.getPostingsReader();
        Signature filter = null;

        //Decide which filter to use based on the configuration parameters
        int df = pl.getDf();
        if (df <= bloomConfig.getIdentityHashThreshold()) {
          filter = new BloomFilterHash(df * bloomConfig.getBitsPerElement(),
                                       bloomConfig.getHashCount());
        } else {
          filter = new BloomFilterIdentityHash(bloomConfig.getDocumentCount());
        }

        while (reader.nextPosting(posting)) {
          filter.add(newDocids[posting.getDocno()]);
        }

        out.writeInt(i);
        out.writeInt(df);
        filter.write(out);
      } catch(Exception e) {
        continue;
      }
    }

    if(out != null) {
      out.close();
    }
  }
}
