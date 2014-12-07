package ivory.ffg.preprocessing;

import ivory.bloomir.util.DocumentUtility;
import ivory.bloomir.util.OptionManager;
import ivory.bloomir.util.QueryUtility;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.core.data.index.TermPositions;
import ivory.core.data.stat.SpamPercentileScore;
import ivory.ffg.data.CompressedPositionalPostings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import tl.lin.data.map.HMapII;
import tl.lin.data.map.HMapIV;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class GenerateCompressedPositionalPostings {
  private static final Logger LOGGER = Logger.getLogger(GenerateCompressedPositionalPostings.class);

  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(GenerateCompressedPositionalPostings.class.getName());
    options.addOption(OptionManager.INDEX_ROOT_PATH, "path", "index root", true);
    options.addOption(OptionManager.OUTPUT_PATH, "path", "output", true);
    options.addOption(OptionManager.QUERY_PATH, "path", "XML query", true);
    options.addOption(OptionManager.SPAM_PATH, "path", "spam percentile scores", true);

    try {
      options.parse(args);
    } catch(Exception exp) {
      return;
    }

    String indexPath = options.getOptionValue(OptionManager.INDEX_ROOT_PATH);
    String outputPath = options.getOptionValue(OptionManager.OUTPUT_PATH);
    String spamPath = options.getOptionValue(OptionManager.SPAM_PATH);
    String queryPath = options.getOptionValue(OptionManager.QUERY_PATH);

    FileSystem fs = FileSystem.get(new Configuration());
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);

    FSDataOutputStream output = fs.create(new Path(outputPath));

    //Parse queries and find integer codes for the query terms.
    HMapIV<String> parsedQueries = QueryUtility.loadQueries(queryPath);
    HMapIV<int[]> queries = QueryUtility.queryToIntegerCode(env, parsedQueries);

    Set<Integer> termidHistory = Sets.newHashSet();
    HMapII docLengths = new HMapII();

    SpamPercentileScore spamScores = new SpamPercentileScore();
    spamScores.initialize(spamPath, fs);
    int[] newDocids = DocumentUtility.spamSortDocids(spamScores);

    Posting posting = new Posting();
    List<TermPositions> positions = Lists.newArrayList();
    Map<Integer, TermPositions> positionsMap = Maps.newHashMap();

    for(int qid: queries.keySet()) {
      for(int termid: queries.get(qid)) {
        if(!termidHistory.contains(termid)) {
          termidHistory.add(termid);
          PostingsList pl = env.getPostingsList(env.getTermFromId(termid));
          PostingsReader reader = pl.getPostingsReader();

          positions.clear();
          positionsMap.clear();
          int[] data = new int[pl.getDf()];
          int index = 0;
          while (reader.nextPosting(posting)) {
            data[index] = newDocids[posting.getDocno()];
            positionsMap.put(data[index], new TermPositions(reader.getPositions(), reader.getTf()));
            docLengths.put(data[index], env.getDocumentLength(posting.getDocno()));
            index++;
          }
          Arrays.sort(data);

          for(int i = 0; i < data.length; i++) {
            positions.add(positionsMap.get(data[i]));
          }

          output.writeInt(termid);
          output.writeInt(pl.getDf());
          CompressedPositionalPostings.newInstance(data, positions).write(output);
        }
      }
      LOGGER.info("Compressed query " + qid);
    }

    output.writeInt(-1);

    output.writeInt(docLengths.size());
    for(int docid: docLengths.keySet()) {
      output.writeInt(docid);
      output.writeInt(docLengths.get(docid));
    }

    output.close();
  }
}
