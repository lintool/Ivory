package ivory.bloomir;

import java.io.File;

import junit.framework.JUnit4TestAdapter;

import com.google.common.io.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import ivory.bloomir.preprocessing.GenerateBloomFilters;
import ivory.bloomir.preprocessing.GenerateCompressedPostings;
import ivory.bloomir.ranker.BloomRanker;
import ivory.bloomir.ranker.SmallAdaptiveRanker;

public class Basic {
  private static final Logger LOG = Logger.getLogger(Basic.class);

  private static final String[] EXACT_RETRIEVAL = {
    "clueweb09-en0000-18-04501",
    "clueweb09-en0000-27-17010",
    "clueweb09-en0000-38-21698",
    "clueweb09-en0000-38-21759",
    "clueweb09-en0000-62-03946",
    "clueweb09-en0000-94-32918",
    "clueweb09-en0001-17-34091",
    "clueweb09-en0001-38-11377",
    "clueweb09-en0001-71-00163",
    "clueweb09-en0001-79-05265",
    "clueweb09-en0006-85-33142",
    "clueweb09-en0006-85-33145",
    "clueweb09-en0006-85-33164",
    "clueweb09-en0006-85-33167",
    "clueweb09-en0006-85-33169",
    "clueweb09-en0006-85-33170",
    "clueweb09-en0006-85-33171",
    "clueweb09-en0006-85-33172",
    "clueweb09-en0006-85-33175",
    "clueweb09-en0006-85-33194",
    "clueweb09-en0000-00-00244",
    "clueweb09-en0000-00-00271",
    "clueweb09-en0000-00-00277",
    "clueweb09-en0000-00-01410",
    "clueweb09-en0000-00-01868",
    "clueweb09-en0000-00-02447",
    "clueweb09-en0000-00-02469",
    "clueweb09-en0000-00-02763",
    "clueweb09-en0000-00-02987",
    "clueweb09-en0000-00-03024",
    "clueweb09-en0000-02-33378",
    "clueweb09-en0000-04-31931",
    "clueweb09-en0000-05-24900",
    "clueweb09-en0000-05-24912",
    "clueweb09-en0000-05-24919",
    "clueweb09-en0000-13-02879",
    "clueweb09-en0000-14-29070",
    "clueweb09-en0000-14-29162",
    "clueweb09-en0000-14-29664",
    "clueweb09-en0000-15-06966",
    "clueweb09-en0000-01-10702",
    "clueweb09-en0000-02-07421",
    "clueweb09-en0000-02-07453",
    "clueweb09-en0000-02-07465",
    "clueweb09-en0000-02-07467",
    "clueweb09-en0000-02-07469",
    "clueweb09-en0000-02-07485",
    "clueweb09-en0000-02-07501",
    "clueweb09-en0000-02-07522",
    "clueweb09-en0000-02-17263",
    "clueweb09-en0001-09-33388",
    "clueweb09-en0001-09-33391",
    "clueweb09-en0001-09-33418",
    "clueweb09-en0001-13-30657",
    "clueweb09-en0001-13-30658",
    "clueweb09-en0001-13-30659",
    "clueweb09-en0001-13-30660",
    "clueweb09-en0001-13-30661",
    "clueweb09-en0001-13-30662",
    "clueweb09-en0001-13-30663",
    "clueweb09-en0000-00-20365",
    "clueweb09-en0000-00-26173",
    "clueweb09-en0000-00-26176",
    "clueweb09-en0000-00-33692",
    "clueweb09-en0000-01-15065",
    "clueweb09-en0000-01-15066",
    "clueweb09-en0000-01-15067",
    "clueweb09-en0000-01-15077",
    "clueweb09-en0000-01-15221",
    "clueweb09-en0000-01-15676",
    "clueweb09-en0000-00-02970",
    "clueweb09-en0000-00-02979",
    "clueweb09-en0000-00-33692",
    "clueweb09-en0000-02-06919",
    "clueweb09-en0000-02-07453",
    "clueweb09-en0000-02-07768",
    "clueweb09-en0000-02-14152",
    "clueweb09-en0000-02-30658",
    "clueweb09-en0000-02-31391",
    "clueweb09-en0000-03-09356",
    "clueweb09-en0000-00-10046",
    "clueweb09-en0000-00-10056",
    "clueweb09-en0000-00-24511",
    "clueweb09-en0000-00-26176",
    "clueweb09-en0000-00-26188",
    "clueweb09-en0000-00-26247",
    "clueweb09-en0000-00-28409",
    "clueweb09-en0000-01-06273",
    "clueweb09-en0000-01-18528",
    "clueweb09-en0000-01-18530",
    "clueweb09-en0000-04-05514",
    "clueweb09-en0000-14-34558",
    "clueweb09-en0000-16-33595",
    "clueweb09-en0000-23-14054",
    "clueweb09-en0000-26-17501",
    "clueweb09-en0000-27-17043",
    "clueweb09-en0000-36-18725",
    "clueweb09-en0000-38-30733",
    "clueweb09-en0000-38-31239",
    "clueweb09-en0000-40-20564",
    "clueweb09-en0000-02-29977",
    "clueweb09-en0000-23-09545",
    "clueweb09-en0000-32-02869",
    "clueweb09-en0001-22-17614",
    "clueweb09-en0002-04-10473",
    "clueweb09-en0002-24-06105",
    "clueweb09-en0002-25-30849",
    "clueweb09-en0002-43-27297",
    "clueweb09-en0002-61-15183",
    "clueweb09-en0002-74-01013",
    "clueweb09-en0000-00-29007",
    "clueweb09-en0000-04-00261",
    "clueweb09-en0000-12-15554",
    "clueweb09-en0000-13-09713",
    "clueweb09-en0000-25-30574",
    "clueweb09-en0000-39-27495",
    "clueweb09-en0000-45-20997",
    "clueweb09-en0000-45-20998",
    "clueweb09-en0000-45-20999",
    "clueweb09-en0000-48-26086",
    "clueweb09-en0000-00-00170",
    "clueweb09-en0000-00-00244",
    "clueweb09-en0000-00-00247",
    "clueweb09-en0000-00-00253",
    "clueweb09-en0000-00-00255",
    "clueweb09-en0000-00-00256",
    "clueweb09-en0000-00-00258",
    "clueweb09-en0000-00-00267",
    "clueweb09-en0000-00-00271",
    "clueweb09-en0000-00-00275",
    "clueweb09-en0000-00-17887",
    "clueweb09-en0000-00-17912",
    "clueweb09-en0000-00-26194",
    "clueweb09-en0000-02-33392",
    "clueweb09-en0000-03-11134",
    "clueweb09-en0000-05-07676",
    "clueweb09-en0000-05-07721",
    "clueweb09-en0000-05-07722",
    "clueweb09-en0000-05-19941",
    "clueweb09-en0000-10-00592",
    "clueweb09-en0000-00-20165",
    "clueweb09-en0000-00-22218",
    "clueweb09-en0000-00-28365",
    "clueweb09-en0000-02-35175",
    "clueweb09-en0000-12-15514",
    "clueweb09-en0000-12-15533",
    "clueweb09-en0000-12-15535",
    "clueweb09-en0000-12-15537",
    "clueweb09-en0000-22-10947",
    "clueweb09-en0000-22-10969",
    "clueweb09-en0000-45-10745",
    "clueweb09-en0000-45-10749",
    "clueweb09-en0000-55-17313",
    "clueweb09-en0000-69-24267",
    "clueweb09-en0000-70-20787",
    "clueweb09-en0000-99-07196",
    "clueweb09-en0000-99-07197",
    "clueweb09-en0000-99-07198",
    "clueweb09-en0000-99-07200",
    "clueweb09-en0000-99-07201",
    "clueweb09-en0000-02-26149",
    "clueweb09-en0000-05-13683",
    "clueweb09-en0000-05-13686",
    "clueweb09-en0000-11-19509",
    "clueweb09-en0000-11-19521",
    "clueweb09-en0000-15-06861",
    "clueweb09-en0000-15-06913",
    "clueweb09-en0000-15-07226",
    "clueweb09-en0000-15-07363",
    "clueweb09-en0000-16-33601",
    "clueweb09-en0000-00-02447",
    "clueweb09-en0000-00-02929",
    "clueweb09-en0000-00-02970",
    "clueweb09-en0000-00-02987",
    "clueweb09-en0000-00-03032",
    "clueweb09-en0000-00-03033",
    "clueweb09-en0000-00-03037",
    "clueweb09-en0000-00-03039",
    "clueweb09-en0000-00-03041",
    "clueweb09-en0000-00-03042",
    "clueweb09-en0000-27-17057",
    "clueweb09-en0000-45-23090",
    "clueweb09-en0000-81-18218",
    "clueweb09-en0000-83-19582",
    "clueweb09-en0000-92-24820",
    "clueweb09-en0001-77-29100",
    "clueweb09-en0002-31-09134",
    "clueweb09-en0002-48-08552",
    "clueweb09-en0002-76-24263",
    "clueweb09-en0002-84-19879",
    "clueweb09-en0000-01-07048",
    "clueweb09-en0000-01-07049",
    "clueweb09-en0000-09-13294",
    "clueweb09-en0000-22-23585",
    "clueweb09-en0000-23-14121",
    "clueweb09-en0000-23-14150",
    "clueweb09-en0000-33-31128",
    "clueweb09-en0000-33-31133",
    "clueweb09-en0000-36-28781",
    "clueweb09-en0000-36-28782",
    "clueweb09-en0000-00-01409",
    "clueweb09-en0000-00-01410",
    "clueweb09-en0000-00-24146",
    "clueweb09-en0000-00-26129",
    "clueweb09-en0000-00-28556",
    "clueweb09-en0000-01-00144",
    "clueweb09-en0000-01-03236",
    "clueweb09-en0000-01-15073",
    "clueweb09-en0000-01-15074",
    "clueweb09-en0000-01-15077",
    "clueweb09-en0000-00-10046",
    "clueweb09-en0000-00-10056",
    "clueweb09-en0000-00-15331",
    "clueweb09-en0000-00-16102",
    "clueweb09-en0000-00-17350",
    "clueweb09-en0000-00-17354",
    "clueweb09-en0000-00-17396",
    "clueweb09-en0000-00-17398",
    "clueweb09-en0000-00-17400",
    "clueweb09-en0000-00-17422",
    "clueweb09-en0000-00-10046",
    "clueweb09-en0000-00-10056",
    "clueweb09-en0000-02-11898",
    "clueweb09-en0000-02-16657",
    "clueweb09-en0000-15-06742",
    "clueweb09-en0000-17-26294",
    "clueweb09-en0000-17-26442",
    "clueweb09-en0000-23-06246",
    "clueweb09-en0000-23-28659",
    "clueweb09-en0000-27-31206",
    "clueweb09-en0000-00-27311",
    "clueweb09-en0000-00-27448",
    "clueweb09-en0000-02-25469",
    "clueweb09-en0000-04-21753",
    "clueweb09-en0000-07-18217",
    "clueweb09-en0000-09-21095",
    "clueweb09-en0000-09-21108",
    "clueweb09-en0000-09-21114",
    "clueweb09-en0000-09-21124",
    "clueweb09-en0000-09-26899",
    "clueweb09-en0000-00-00258",
    "clueweb09-en0000-00-00275",
    "clueweb09-en0000-00-01859",
    "clueweb09-en0000-00-01862",
    "clueweb09-en0000-00-01863",
    "clueweb09-en0000-00-01864",
    "clueweb09-en0000-00-01869",
    "clueweb09-en0000-00-02148",
    "clueweb09-en0000-00-03030",
    "clueweb09-en0000-00-03043" };

  private static final String IVORY_INDEX_PATH = "/scratch0/indexes/clue.en.01.nopos/";
  private static final String SPAM_PATH = "/scratch0/nima/docscores-spam.dat.en.01";

  @Test public void runRegression() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    File postingIndex = Files.createTempDir();
    File bloomIndex = Files.createTempDir();
    File postingOutput = File.createTempFile("bloomirPO", null);
    File bloomOutput = File.createTempFile("bloomirBO", null);

    String[] paramsCompressedPostings = new String[] {
      "-index", Basic.IVORY_INDEX_PATH,
      "-spam", Basic.SPAM_PATH,
      "-output", postingIndex.getPath()
    };

    String[] paramsBloomFilters = new String[] {
      "-index", Basic.IVORY_INDEX_PATH,
      "-spam", Basic.SPAM_PATH,
      "-output", bloomIndex.getPath(),
      "-bpe", "8",
      "-nbHash", "1"
    };

    GenerateCompressedPostings postingsGenerator = new GenerateCompressedPostings();
    long start = System.currentTimeMillis();
    postingsGenerator.main(paramsCompressedPostings);
    long end = System.currentTimeMillis();
    LOG.info("Total postings generation time: " + (end - start) + "ms");

    GenerateBloomFilters bloomGenerator = new GenerateBloomFilters();
    start = System.currentTimeMillis();
    bloomGenerator.main(paramsBloomFilters);
    end = System.currentTimeMillis();
    LOG.info("Total Bloom filter generation time: " + (end - start) + "ms");

    String[] paramsSARanker = new String[] {
      "-index", Basic.IVORY_INDEX_PATH,
      "-posting", postingIndex.getPath(),
      "-query", "data/clue/queries.web09.1-25.xml",
      "-spam", Basic.SPAM_PATH,
      "-output", postingOutput.getPath(),
      "-hits", "10"
    };

    String[] paramsBloomRanker = new String[] {
      "-index", Basic.IVORY_INDEX_PATH,
      "-posting", postingIndex.getPath(),
      "-bloom", bloomIndex.getPath(),
      "-query", "data/clue/queries.web09.1-25.xml",
      "-spam", Basic.SPAM_PATH,
      "-output", bloomOutput.getPath(),
      "-hits", "1000"
    };

    SmallAdaptiveRanker.main(paramsSARanker);
    BloomRanker.main(paramsBloomRanker);

    FSDataInputStream saInput = fs.open(new Path(postingOutput.getPath()));
    String line;
    int i = 0;
    while((line = saInput.readLine()) != null) {
      if(line.startsWith("<judgment")) {
        String docid = line.split("\"")[3];
        assertEquals(EXACT_RETRIEVAL[i++], docid);
      }
    }
    saInput.close();
    assertEquals(EXACT_RETRIEVAL.length, i);
    LOG.info("Small Adaptive output checked.");

    FSDataInputStream bInput = fs.open(new Path(bloomOutput.getPath()));
    int c = 0, lastQid = -1;
    i = 0;
    while((line = bInput.readLine()) != null) {
      if(line.startsWith("<judgment")) {
        int qid = Integer.parseInt(line.split("\"")[1]);
        if(qid != lastQid) {
          lastQid = qid;
          c = 0;
        }

        if(c < 10) {
          String docid = line.split("\"")[3];
          if(EXACT_RETRIEVAL[i].equals(docid)) {
            i++;
            c++;
          }
        }
      }
    }
    bInput.close();
    assertEquals(EXACT_RETRIEVAL.length, i);

    fs.delete(new Path(postingIndex.getPath()), true);
    fs.delete(new Path(postingOutput.getPath()), true);
    fs.delete(new Path(bloomIndex.getPath()), true);
    fs.delete(new Path(bloomOutput.getPath()), true);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(Basic.class);
  }
}
