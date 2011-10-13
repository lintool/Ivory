package ivory.pwsim;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.core.data.stat.DocLengthTable;
import ivory.core.data.stat.DocLengthTable2B;
import ivory.pwsim.score.ScoringModel;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapIF;

/**
 * <p>
 * Computing pairwise document similarity given a document-sorted inverted
 * index. This implementation is based on the algorithms described in the
 * following papers:
 * </p>
 * 
 * <ul>
 * 
 * <li>Tamer Elsayed, Jimmy Lin, and Douglas Oard. <b><a
 * href="http://www.aclweb.org/anthology/P/P08/P08-2067.pdf">Pairwise Document
 * Similarity in Large Collections with MapReduce.</a></b> Proceedings of the
 * 46th Annual Meeting of the Association for Computational Linguistics (ACL
 * 2008), Companion Volume, pages 265-268, June 2008, Columbus, Ohio.
 * 
 * <li>Jimmy Lin. <b><a
 * href="http://portal.acm.org/citation.cfm?id=1571941.1571970">Brute Force and
 * Indexed Approaches to Pairwise Document Similarity Comparisons with
 * MapReduce.</a></b> Proceedings of the 32nd Annual International ACM SIGIR
 * Conference on Research and Development in Information Retrieval (SIGIR 2009),
 * pages 155-162, July 2009, Boston, Massachusetts.
 * 
 * </ul>
 * 
 * @author Tamer Elsayed
 * @author Jimmy Lin
 * 
 */
public class PCP extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(PCP.class);
	{
		sLogger.setLevel(Level.INFO);
	}

	private static class MyMapper extends MapReduceBase implements
			Mapper<IntWritable, PostingsList, IntWritable, HMapIFW> {

		// table that contains length of all document, to be used in computing
		// similarity
		private DocLengthTable mDocLengthTable;

		// similarity measure
		private ScoringModel mModel;

		// threshold to filter common terms that don't contribute much in
		// similarities
		private int dfCut;

		// starting row (in similarity matrix) to be computed
		private int mBlockStart;

		// ending row (in similarity matrix) to be computed
		private int mBlockEnd;

		// collection size
		private int mCollectionDocCount;

		public void configure(JobConf job) {
			mCollectionDocCount = job.getInt("Ivory.CollectionDocumentCount", -1);

			try {
				if (job.get("mapred.job.tracker").equals("local")) {
					FileSystem fs = FileSystem.getLocal(job);
					RetrievalEnvironment re = new RetrievalEnvironment(job.get("Ivory.IndexPath"),
							fs);
					Path path = re.getDoclengthsData();
					sLogger.debug("Reading doclengths: " + path);
					mDocLengthTable = new DocLengthTable2B(path, fs);
				} else {
					Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
					mDocLengthTable = new DocLengthTable2B(localFiles[0], FileSystem.getLocal(job));
				}
			} catch (Exception e) {
				throw new RuntimeException("Error initializing DocLengthTable!");
			}

			dfCut = job.getInt("Ivory.DfCut", -1);
			mBlockStart = job.getInt("Ivory.BlockStart", -1);
			mBlockEnd = job.getInt("Ivory.BlockEnd", -1);
			if (dfCut <= 0 || mBlockStart < 0 || mBlockEnd <= 0)
				throw new RuntimeException("Invalid config parameter(s): dfCut=" + dfCut
						+ ", blockStart=" + mBlockStart + ", blockEnd=" + mBlockEnd);

			try {
				mModel = (ScoringModel) Class.forName(job.get("Ivory.ScoringModel")).newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Mappers failed to initialize!");
			}
			// this only needs to be set once for the entire collection
			mModel.setDocCount(mDocLengthTable.getDocCount());
			mModel.setAvgDocLength(mDocLengthTable.getAvgDocLength());

		}

		Posting e1 = new Posting();
		Posting e2 = new Posting();

		public void map(IntWritable key, PostingsList postings,
				OutputCollector<IntWritable, HMapIFW> output, Reporter reporter) throws IOException {

			sLogger.debug(mCollectionDocCount);
			postings.setCollectionDocumentCount(mCollectionDocCount);

			PostingsReader reader1 = postings.getPostingsReader();

			if (reader1.getNumberOfPostings() > dfCut)
				return;

			// set per postings list
			mModel.setDF(reader1.getNumberOfPostings());

			// performing PCP
			while (reader1.nextPosting(e1)) {

				// Here's a hidden dependency: How we do the blocking depends on
				// how the postings are sorted. If the postings are sorted in
				// ascending docno, then we can break out of the loop after
				// we've gone past the block bounds (as in code below).
				// Otherwise (say, if postings are sorted by tf), we have to go
				// through all postings.
				// -- Jimmy, 2008/09/03
				//

				if (e1.getDocno() < mBlockStart)
					continue;
				if (e1.getDocno() >= mBlockEnd)
					break;

				HMapIFW map = new HMapIFW();

				sLogger.debug(key + ": " + e1);

				PostingsReader reader2 = postings.getPostingsReader();

				while (reader2.nextPosting(e2)) {

					sLogger.debug(key + ": " + e1 + ", " + e2);

					if (e1.getDocno() == e2.getDocno())
						continue;

					// compute partial score of similarity for a pair of
					// documents
					float weight = mModel.computeScore(e1.getTf(), e2.getTf(),
							mDocLengthTable.getDocLength(e1.getDocno()), mDocLengthTable
									.getDocLength(e2.getDocno()));

					map.put(e2.getDocno(), weight);
				}
				output.collect(new IntWritable(e1.getDocno()), map);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<IntWritable, HMapIFW, IntWritable, HMapIFW> {

		HMapIFW map = new HMapIFW();
		HMapIFW newMap = new HMapIFW();
		int topN = -1;

		public void configure(JobConf job) {
			topN = job.getInt("Ivory.TopN", -1);
		}

		public void reduce(IntWritable doc, Iterator<HMapIFW> values,
				OutputCollector<IntWritable, HMapIFW> output, Reporter reporter) throws IOException {

			map.clear();
			while (values.hasNext()) {
				map.plus(values.next());
			}
			newMap.clear();
			if (topN > 0) {
				// get only top N similar documents
				int i = 0;
				for (MapIF.Entry e : map.getEntriesSortedByValue()) {
					if (i >= topN)
						break;
					newMap.put(e.getKey(), e.getValue());
					i++;
				}
			} else {
				for (MapIF.Entry e : map.getEntriesSortedByValue())
					newMap.put(e.getKey(), e.getValue());
			}

			// note: output is not sorted but will only include top N if needed
			output.collect(doc, newMap);
		}
	}

	public PCP(Configuration conf) {
		super(conf);
	}

	public static final String[] RequiredParameters = {
			"Ivory.IndexPath",
			"Ivory.OutputPath",
			"Ivory.NumMapTasks",
			"Ivory.NumReduceTasks",
			"Ivory.ScoringModel",
			"Ivory.DfCut",
			"Ivory.BlockSize",
			"Ivory.TopN"
	};

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public int runTool() throws Exception {
		String indexPath = getConf().get("Ivory.IndexPath");
		String outputPath = getConf().get("Ivory.OutputPath");

		int mapTasks = getConf().getInt("Ivory.NumMapTasks", 0);
		int reduceTasks = getConf().getInt("Ivory.NumReduceTasks", 0);
		int dfCut = getConf().getInt("Ivory.DfCut", -1);
		int blockSize = getConf().getInt("Ivory.BlockSize", -1);
		int topN = getConf().getInt("Ivory.TopN", -1);

		FileSystem fs = FileSystem.get(getConf());

		RetrievalEnvironment re = new RetrievalEnvironment(indexPath, fs);

		String collectionName = re.readCollectionName();
		int numDocs = re.readCollectionDocumentCount();
		Path docLengthPath = re.getDoclengthsData();
		String scoringModel = getConf().get("Ivory.ScoringModel");

		sLogger.info("Characteristics of the collection:");
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info("Characteristics of the job:");
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: " + reduceTasks);
		sLogger.info(" - DfCut: " + getConf().getInt("Ivory.DfCut", 0));
		sLogger.info(" - BlockSize: " + blockSize);
		sLogger.info(" - ScoringModel: " + scoringModel);
		sLogger.info(" - topN: " + topN);
		sLogger.info(" - OutputPath: " + outputPath);

		getConf().setInt("Ivory.CollectionDocumentCount", numDocs);

		if (fs.exists(new Path(outputPath))) {
			System.out.println("PCP output path already exists!");
			return 0;
		}

		int numBlocks = numDocs / blockSize + 1;

		for (int i = 0; i < numBlocks; i++) {
			int start = blockSize * i;
			int end = i == numBlocks - 1 ? numDocs : blockSize * (i + 1);

			JobConf conf = new JobConf(getConf(), PCP.class);
			DistributedCache.addCacheFile(docLengthPath.toUri(), conf);

			sLogger.info("block " + i + ": " + start + "-" + end);

			conf.setInt("Ivory.BlockStart", start);
			conf.setInt("Ivory.BlockEnd", end);

			conf.setJobName("PCP:" + collectionName + "-dfCut=" + dfCut
					+ (topN > 0 ? "-topN" + topN : "-all") + ":Block #" + i);

			conf.setNumMapTasks(mapTasks);
			conf.setNumReduceTasks(reduceTasks);

			String currentOutputPath = outputPath + "/block" + i;

			FileInputFormat.setInputPaths(conf, new Path(re.getPostingsDirectory()));
			FileOutputFormat.setOutputPath(conf, new Path(currentOutputPath));

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(HMapIFW.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);

			conf.setMapperClass(MyMapper.class);
			conf.setCombinerClass(IdentityReducer.class);
			conf.setReducerClass(MyReducer.class);

			JobClient.runJob(conf);
		}

		return 0;
	}

}