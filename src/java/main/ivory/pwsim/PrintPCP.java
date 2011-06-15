package ivory.pwsim;

import ivory.data.DocLengthTable;
import ivory.data.DocLengthTable2B;
import ivory.data.Posting;
import ivory.data.PostingsList;
import ivory.data.PostingsReader;
import ivory.pwsim.score.ScoringModel;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.collection.aquaint2.Aquaint2Document;
import edu.umd.cloud9.collection.aquaint2.Aquaint2ForwardIndex;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapIF;
import edu.umd.cloud9.collection.DocumentForwardIndex;



public class PrintPCP extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(PrintPCP.class);
	{
		sLogger.setLevel(Level.INFO);
	}

	private static class MyMapper extends MapReduceBase implements
            Mapper<IntWritable, HMapIFW, Text, Text> {


		private DocnoMapping mapping;
		private DocumentForwardIndex<Aquaint2Document> mForwardIndex;

		private static Text sKey = new Text ();
		private static Text sValue = new Text ();

		public void configure (JobConf job) {
			try {
				FileSystem fs = FileSystem.get (job);
				String indexPath = job.get ("Ivory.IndexPath");
				RetrievalEnvironment env = new RetrievalEnvironment (indexPath, fs);
				mapping = RetrievalEnvironment.loadDocnoMapping (indexPath, fs);
				mForwardIndex = new Aquaint2ForwardIndex ();
				String findexFilePath = indexPath + "/findex.dat";
				//String findexMappingFilePath = indexPath + "/findex/part-00000";
				String findexMappingFilePath = indexPath + "/docno-mapping.dat";
				mForwardIndex.loadIndex (findexFilePath, findexMappingFilePath);

			} catch (IOException e) {
				e.printStackTrace ();
			}
		}


		public void map (IntWritable docnoW, HMapIFW docMap,
						 OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			int docno = docnoW.get ();
			String docid = mapping.getDocid (docno);
			Aquaint2Document doc = mForwardIndex.getDocument (docid);
			String docHeadline = doc.getHeadline ();
			sKey.set (docid + " " + docHeadline);

			for (MapIF.Entry entry : docMap.getEntriesSortedByValue (10)) {
				int otherDocno = entry.getKey ();
				float weight = entry.getValue ();
				String otherDocid = mapping.getDocid (otherDocno);
				Aquaint2Document otherDoc = mForwardIndex.getDocument (otherDocid);
				String otherDocHeadline = otherDoc.getHeadline ();
				sValue.set (weight + ": " + otherDocid + " " + otherDocHeadline);
				output.collect (sKey, sValue);
			}
		}
	}


	public PrintPCP (Configuration conf) {
		super (conf);
	}

	public static final String[] RequiredParameters = {
			"Ivory.IndexPath",
			"Ivory.OutputPath",
			"Ivory.ResultsOutputPath",
	};

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}


	public int runTool() throws Exception {

		JobConf conf = new JobConf (getConf (), PrintPCP.class);
		FileSystem fs = FileSystem.get (conf);

		int mapTasks = conf.getInt ("Ivory.NumMapTasks", 1);
		int reduceTasks = conf.getInt ("Ivory.NumReduceTasks", 1);

		String outputPath = getConf().get("Ivory.OutputPath");
		String resultsOutputPath = getConf().get("Ivory.ResultsOutputPath");

		FileInputFormat.setInputPaths (conf, new Path (outputPath + "/block0/part-00000"));
		FileOutputFormat.setOutputPath (conf, new Path (resultsOutputPath));

		conf.setNumMapTasks (mapTasks);
		conf.setNumReduceTasks (reduceTasks);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat (TextOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		JobClient.runJob (conf);

		return 0;
	}

}
