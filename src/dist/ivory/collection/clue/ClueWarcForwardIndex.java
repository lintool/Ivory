/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.collection.clue;

import ivory.data.DocumentForwardIndex;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import edu.umd.cloud9.util.ExceptionUtils;
import edu.umd.cloud9.util.FSLineReader;

/**
 * <p>
 * Object representing a document forward index for the ClueWeb09 collection.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class ClueWarcForwardIndex implements DocumentForwardIndex<ClueWarcRecord> {

	private static final Logger sLogger = Logger.getLogger(ClueWarcForwardIndex.class);

	private static Pattern p = Pattern.compile("(part-\\d+)\\s+(clueweb[^\\s]+)\\s+(\\d+)");

	private Map<String, String> mClueSectionIdToPartMapping = new HashMap<String, String>();
	private Map<String, Map<Integer, Long>> mClueByteOffsetMapping = new HashMap<String, Map<Integer, Long>>();

	private int interval = 1000;
	private String mCollectionBasePath;
	private DocnoMapping mDocnoMapping;

	public ClueWarcForwardIndex() {
	}

	public String getContentType() {
		return "text/html";
	}

	public ClueWarcRecord getDocid(String docid) {
		return getRecord(docid);
	}

	public ClueWarcRecord getDocno(int docno) {
		return getRecord(mDocnoMapping.getDocid(docno));
	}

	public ClueWarcRecord getRecord(String docid) {
		try {
			sLogger.info("fetching docid " + docid);
			long startTime = System.currentTimeMillis();

			String clueSectionId = docid.substring(10, 19);
			int clueDocCntInSection = Integer.parseInt(docid.substring(20, 25));
			int start = clueDocCntInSection / interval * interval;
			int remaining = (clueDocCntInSection - start) + 1;

			String file = mCollectionBasePath + "/"
					+ mClueSectionIdToPartMapping.get(clueSectionId);

			sLogger.info("file: " + file);
			sLogger.info("closest record in file: " + start);

			long seekPos = mClueByteOffsetMapping.get(clueSectionId).get(start);

			sLogger.info("seek position: " + seekPos);
			sLogger.info("seek cnt: " + remaining);

			Path path = new Path(file);
			Configuration config = new Configuration();
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), path,
					config);

			LongWritable key = new LongWritable();
			ClueWarcRecord value = new ClueWarcRecord();

			if (seekPos != 0)
				reader.seek(seekPos);

			for (int i = 0; i < remaining; i++)
				reader.next(key, value);

			reader.close();

			long endTime = System.currentTimeMillis();
			sLogger.info("record fecthed in " + (endTime - startTime) + " ms");

			return value;
		} catch (Exception e) {
			sLogger.error("Error fetching docid!");
			sLogger.error(ExceptionUtils.getStackTrace(e));

			return null;
		}
	}

	public void loadIndex(DocnoMapping mapping, String forwardIndexFile, String collectionBase) {
		mDocnoMapping = mapping;
		mCollectionBasePath = collectionBase;

		try {
			FSLineReader reader = new FSLineReader(forwardIndexFile);

			Text t = new Text();
			while (reader.readLine(t) != 0) {
				Matcher matcher = p.matcher(t.toString());
				if (!matcher.find())
					continue;

				String part = matcher.group(1);
				String docid = matcher.group(2);
				String clueSectionId = docid.substring(10, 19);
				int clueDocCntInSection = Integer.parseInt(docid.substring(20, 25));
				long byteOffset = Long.parseLong(matcher.group(3));

				if (clueDocCntInSection == 0) {
					sLogger.info(part + ": " + clueSectionId);

					mClueSectionIdToPartMapping.put(clueSectionId, part);
					Map<Integer, Long> map = new HashMap<Integer, Long>();
					map.put(clueDocCntInSection, byteOffset);
					mClueByteOffsetMapping.put(clueSectionId, map);
				} else {
					mClueByteOffsetMapping.get(clueSectionId).put(clueDocCntInSection, byteOffset);
				}
			}

			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
