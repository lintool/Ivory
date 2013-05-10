package ivory.integration.irj2012;

import java.io.File;

import junit.framework.JUnit4TestAdapter;

import com.google.common.io.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import ivory.ffg.preprocessing.GenerateDocumentVectors;
import ivory.ffg.preprocessing.GenerateCompressedPositionalPostings;
import ivory.ffg.driver.DocumentVectorOnTheFlyIndexing;
import ivory.ffg.driver.RankAndFeaturesSmallAdaptive;

public class VerifyFeatureExtraction {
  private static final Logger LOG = Logger.getLogger(VerifyFeatureExtraction.class);

  private static final String[] FEATURES = {
    "1	622182	0.0 8.5106735 0.0 -21.956047 -20.940353 -21.956047 ",
    "1	939420	0.0 8.111909 0.0 -22.17752 -21.539658 -22.17752 ",
    "1	1309504	0.0 10.92844 0.0 -23.21772 -18.169184 -23.21772 ",
    "1	1309565	0.0 8.300849 0.0 -23.40636 -21.933271 -23.40636 ",
    "1	2107067	4.5951195 10.087684 4.5951195 -17.570501 -18.763742 -17.570501 ",
    "1	3213263	0.0 8.32819 0.0 -22.91655 -21.601917 -22.91655 ",
    "1	3991819	0.0 10.222314 0.0 -21.487642 -18.61485 -21.487642 ",
    "1	4677357	0.0 8.744904 0.0 -21.57198 -20.75782 -21.57198 ",
    "1	5778775	0.0 7.911496 0.0 -22.035013 -21.80349 -22.035013 ",
    "1	6043395	0.0 7.835582 0.0 -22.71848 -21.526793 -22.71848 ",
    "2	24249210	15.735939 22.211124 15.692177 -16.604282 -20.48779 -16.78569 ",
    "2	24249213	15.214951 20.727518 15.214951 -17.903378 -23.020555 -17.903378 ",
    "2	24249232	14.704382 22.109085 10.109262 -18.818066 -21.37287 -22.436802 ",
    "2	24249235	14.596262 21.126617 10.0011425 -18.759815 -23.215107 -22.378551 ",
    "2	24249237	10.140953 20.56073 10.140953 -22.563118 -23.267324 -22.563118 ",
    "2	24249238	10.619831 20.861208 10.619831 -21.24904 -22.737915 -21.24904 ",
    "2	24249239	10.211376 20.72454 10.211376 -21.987541 -22.518667 -21.987541 ",
    "2	24249240	10.109262 20.376194 10.109262 -22.579971 -23.695538 -22.579971 ",
    "2	24249243	14.596262 21.139387 15.106831 -19.567122 -23.415813 -18.887474 ",
    "2	24249262	15.570984 20.983067 15.570984 -17.326145 -22.088848 -17.326145 ",
    "3	245	0.0 1.8352408 0.0 0.0 -6.8256583 0.0 ",
    "3	272	0.0 1.8170375 0.0 0.0 -6.861235 0.0 ",
    "3	278	0.0 1.9549259 0.0 0.0 -6.575493 0.0 ",
    "3	1411	0.0 1.7040439 0.0 0.0 -7.071218 0.0 ",
    "3	1869	0.0 2.3621986 0.0 0.0 -5.9624248 0.0 ",
    "3	2448	0.0 2.0067103 0.0 0.0 -6.791424 0.0 ",
    "3	2470	0.0 1.6766067 0.0 0.0 -7.1198583 0.0 ",
    "3	2764	0.0 1.8759606 0.0 0.0 -6.7439375 0.0 ",
    "3	2988	0.0 2.1518514 0.0 0.0 -6.455552 0.0 ",
    "3	3025	0.0 1.9815545 0.0 0.0 -6.5151377 0.0 ",
    "4	97374	0.0 6.3133283 0.0 0.0 -6.3661027 0.0 ",
    "4	168240	0.0 5.2482157 0.0 0.0 -7.131438 0.0 ",
    "4	195995	0.0 4.8623586 0.0 0.0 -7.409898 0.0 ",
    "4	196007	0.0 5.2050014 0.0 0.0 -7.1644793 0.0 ",
    "4	196014	0.0 4.8719454 0.0 0.0 -7.4033713 0.0 ",
    "4	442831	0.0 4.7180376 0.0 0.0 -7.5062284 0.0 ",
    "4	502657	0.0 5.027849 0.0 0.0 -7.294698 0.0 ",
    "4	502749	0.0 5.010241 0.0 0.0 -7.3072276 0.0 ",
    "4	503251	0.0 5.15173 0.0 0.0 -7.204485 0.0 ",
    "4	517535	0.0 4.817546 0.0 0.0 -7.440185 0.0 ",
    "5	46285	0.0 5.6725864 0.0 -12.171908 -15.081589 -12.171908 ",
    "5	71417	0.0 7.576192 0.0 -10.463681 -13.751675 -10.463681 ",
    "5	71449	0.0 7.529799 0.0 -11.411591 -14.297478 -11.411591 ",
    "5	71461	0.0 9.100533 0.0 -10.601865 -12.391013 -10.601865 ",
    "5	71463	0.0 7.5188546 0.0 -10.5033 -13.814863 -10.5033 ",
    "5	71465	0.0 7.4566336 0.0 -10.545237 -13.882295 -10.545237 ",
    "5	71481	0.0 7.591035 0.0 -10.453264 -13.735146 -10.453264 ",
    "5	71497	0.0 8.521038 0.0 -10.388376 -12.953309 -10.388376 ",
    "5	71518	0.0 7.430893 0.0 -10.562283 -13.90986 -10.562283 ",
    "5	81259	0.0 5.893096 0.0 -11.648296 -15.3101015 -11.648296 "
  };

  private static final String[] VECTOR_TYPES = {
    "ivory.ffg.data.DocumentVectorHashedArray",
    "ivory.ffg.data.DocumentVectorVIntArray",
    "ivory.ffg.data.DocumentVectorPForDeltaArray",
    "ivory.ffg.data.DocumentVectorMiniInvertedIndex"
  };

  private static final String IVORY_INDEX_PATH = "/scratch0/indexes/adhoc/clue.en.01/";
  private static final String SPAM_PATH = "/scratch0/indexes/adhoc/CIKM2012/docscores-spam.dat.en.01";
  private static final String QUERY_PATH = "data/ivory/ffg/queries.xml";
  private static final String FEATURES_PATH = "data/ivory/ffg/features.xml";
  private static final String DOCUMENTS_PATH = "data/ivory/ffg/documents.txt";

  @Test public void runRegression() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    File vectors = File.createTempFile("ffgDocumentVector", null);
    File features = File.createTempFile("ffgFeatures", null);

    // Verify Monolithic (Small Adaptive to retrieve and compute features)
    String[] paramsCompressedPostings = new String[] {
      "-index", VerifyFeatureExtraction.IVORY_INDEX_PATH,
      "-spam", VerifyFeatureExtraction.SPAM_PATH,
      "-query", VerifyFeatureExtraction.QUERY_PATH,
      "-output", vectors.getPath()
    };

    String[] paramsMonolithic = new String[] {
      "-index", VerifyFeatureExtraction.IVORY_INDEX_PATH,
      "-posting", vectors.getPath(),
      "-query", VerifyFeatureExtraction.QUERY_PATH,
      "-judgment", VerifyFeatureExtraction.DOCUMENTS_PATH,
      "-feature", VerifyFeatureExtraction.FEATURES_PATH,
      "-hits", "10",
      "-spam", VerifyFeatureExtraction.SPAM_PATH,
      "-output", features.getPath()
    };

    GenerateCompressedPositionalPostings postingsGenerator =
      new GenerateCompressedPositionalPostings();
    long start = System.currentTimeMillis();
    postingsGenerator.main(paramsCompressedPostings);
    long end = System.currentTimeMillis();
    LOG.info("Total postings generation time: " + (end - start) + "ms");

    RankAndFeaturesSmallAdaptive.main(paramsMonolithic);

    FSDataInputStream input = fs.open(new Path(features.getPath()));
    String line;
    int i = 0;
    while((line = input.readLine()) != null) {
      assertTrue(FEATURES[i++].trim().equals(line.trim()));
    }
    input.close();
    assertEquals(FEATURES.length, i);
    LOG.info("Monolithic output checked.");

    // Verify Document Vectors
    for(String vectorType: VECTOR_TYPES) {
      String[] paramsDocumentVector = new String[] {
        "-index", VerifyFeatureExtraction.IVORY_INDEX_PATH,
        "-dvclass", vectorType,
        "-judgment", VerifyFeatureExtraction.DOCUMENTS_PATH,
        "-output", vectors.getPath()
      };

      String[] paramsDecoupled = new String[] {
        "-index", VerifyFeatureExtraction.IVORY_INDEX_PATH,
        "-dvclass", vectorType,
        "-document", vectors.getPath(),
        "-query", VerifyFeatureExtraction.QUERY_PATH,
        "-judgment", VerifyFeatureExtraction.DOCUMENTS_PATH,
        "-feature", VerifyFeatureExtraction.FEATURES_PATH,
        "-output", features.getPath()
      };

      GenerateDocumentVectors dvGenerator =
        new GenerateDocumentVectors();
      start = System.currentTimeMillis();
      dvGenerator.main(paramsDocumentVector);
      end = System.currentTimeMillis();
      LOG.info("Total document vector generation time: " + (end - start) + "ms");

      DocumentVectorOnTheFlyIndexing.main(paramsDecoupled);

      input = fs.open(new Path(features.getPath()));
      i = 0;
      while((line = input.readLine()) != null) {
        assertTrue(FEATURES[i++].trim().equals(line.trim()));
      }
      input.close();
      assertEquals(FEATURES.length, i);
      LOG.info(vectorType + " output checked.");
    }

    fs.delete(new Path(vectors.getPath()), true);
    fs.delete(new Path(features.getPath()), true);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyFeatureExtraction.class);
  }
}
