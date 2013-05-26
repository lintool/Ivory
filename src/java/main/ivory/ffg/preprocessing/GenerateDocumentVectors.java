package ivory.ffg.preprocessing;

import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.map.HMapIV;

import ivory.bloomir.util.OptionManager;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.IntDocVector;
import ivory.ffg.data.DocumentVectorUtility;
import ivory.ffg.util.QrelUtility;

/**
 * @author Nima Asadi
 */
public class GenerateDocumentVectors {
  private static final Logger LOGGER = Logger.getLogger(GenerateDocumentVectors.class);

  public static void main(String[] args) throws Exception {
    OptionManager options = new OptionManager(GenerateDocumentVectors.class.getName());
    options.addOption(OptionManager.INDEX_ROOT_PATH, "path", "index root", true);
    options.addOption(OptionManager.DOCUMENT_VECTOR_CLASS, "class_name", "documentVector class", true);
    options.addOption(OptionManager.OUTPUT_PATH, "path", "output", true);
    options.addOption(OptionManager.JUDGMENT_PATH, "path", "Tab-Delimited documents", true);

    try {
      options.parse(args);
    } catch(Exception exp) {
      return;
    }

    String indexPath = options.getOptionValue(OptionManager.INDEX_ROOT_PATH);
    String documentVectorClass = options.getOptionValue(OptionManager.DOCUMENT_VECTOR_CLASS);
    String outputPath = options.getOptionValue(OptionManager.OUTPUT_PATH);
    String qrelPath = options.getOptionValue(OptionManager.JUDGMENT_PATH);

    FileSystem fs = FileSystem.get(new Configuration());
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);

    //Parse queries, judgemnts and features
    HMapIV<int[]> qrels = QrelUtility.parseQrelsFromTabDelimited(qrelPath);

    FSDataOutputStream output = fs.create(new Path(outputPath));
    Set<Integer> docidHistory = Sets.newHashSet();

    //Evaluate queries and/or write the results to an output file
    for(int qid: qrels.keySet()) {
      for(int docid: qrels.get(qid)) {
        if(!docidHistory.contains(docid)) {
          docidHistory.add(docid);

          IntDocVector vector = env.documentVectors(new int[]{docid})[0];
          output.writeInt(docid);
          DocumentVectorUtility.newInstance(documentVectorClass, vector).write(output);
        }
      }
      LOGGER.info("Compressed query " + qid);
    }

    output.writeInt(-1);
    output.close();
  }
}
