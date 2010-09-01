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

package ivory.http.server;

import ivory.data.DocumentForwardIndex;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.Indexable;

/**
 * @author Tamer Elsayed
 */
public class FetchDocnoServlet extends HttpServlet {
	static final long serialVersionUID = 3986721097L;
	private static final Logger sLogger = Logger.getLogger(FetchDocnoServlet.class);

	public static final String ACTION = "/FetchDocno";
	public static final String DOCNO_FIELD = "docno";

	private DocumentForwardIndex<Indexable> mFowardIndex;
	private DocnoMapping mDocnoMapping;

	@SuppressWarnings("unchecked")
	public void init(ServletConfig config) throws ServletException {
		sLogger.info("initializing servlet for fetching docnos...");

		String indexPath = (String) config.getServletContext().getAttribute("IndexPath");

		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error getting file system!");
		}

		String collectionPath = RetrievalEnvironment.readCollectionPath(fs, indexPath);
		String fwindexFile = RetrievalEnvironment.getDocumentForwardIndex(indexPath);
		String docnoMappingClass = RetrievalEnvironment.readDocnoMappingClass(fs, indexPath);
		String docnoMappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);

		String indexClass = RetrievalEnvironment.readDocumentForwardIndexClass(fs, indexPath);
		String indexFile = RetrievalEnvironment.getDocumentForwardIndex(indexPath);

		sLogger.info("forward index: " + fwindexFile);
		sLogger.info("base path of collection: " + collectionPath);
		sLogger.info("docno mapping file: " + docnoMappingFile);
		sLogger.info("forward index class: " + indexClass);

		try {
			mDocnoMapping = (DocnoMapping) Class.forName(docnoMappingClass).newInstance();
			mDocnoMapping.loadMapping(new Path(docnoMappingFile), fs);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error initializing DocnoMapping!");
		}

		try {
			mFowardIndex = (DocumentForwardIndex<Indexable>) Class.forName(indexClass)
					.newInstance();
			mFowardIndex.loadIndex(mDocnoMapping, indexFile, collectionPath);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error initializing forward index!");
		}
	}

	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
			IOException {
		doPost(req, res);
	}

	public void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException,
			IOException {
		res.setContentType(mFowardIndex.getContentType());
		PrintWriter out = res.getWriter();

		String docno = null;
		if (req.getParameterValues(DOCNO_FIELD) != null)
			docno = req.getParameterValues(DOCNO_FIELD)[0];

		sLogger.info("fetching docno " + docno);

		long start = System.currentTimeMillis();
		Indexable doc = mFowardIndex.getDocno(Integer.parseInt(docno));
		long duration = System.currentTimeMillis() - start;

		if (doc != null) {
			sLogger.info("fetched " + doc.getDocid() + " in " + duration + " ms");
			out.print(doc.getContent());
		} else {
			sLogger.info("trapped error fetching " + docno);
		}

		out.close();
	}

	public static String formatRequestURL(int docno) {
		return ACTION + "?" + DOCNO_FIELD + "=" + new Integer(docno).toString();
	}

}