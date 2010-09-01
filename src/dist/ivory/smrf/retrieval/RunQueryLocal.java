/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

package ivory.smrf.retrieval;

import java.io.IOException;
import java.rmi.NotBoundException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

public class RunQueryLocal {

	private static final Logger sLogger = Logger.getLogger(RunQueryLocal.class);

	private BatchQueryRunner runner = null;

	public RunQueryLocal(String[] args) throws SAXException, IOException,
			ParserConfigurationException, Exception, NotBoundException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.getLocal(conf);
		try {
			sLogger.info("initilaize runquery ...");
			runner = new BatchQueryRunner(args, fs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * runs the queries
	 */
	public void runQueries() throws Exception {
		// run the queries
		sLogger.info("run the queries ...");
		long start = System.currentTimeMillis();
		runner.runQueries();
		long end = System.currentTimeMillis();

		sLogger.info("Total query time: " + (end - start) + "ms");
	}

	public static void main(String[] args) throws Exception {
		RunQueryLocal s;
		try {
			s = new RunQueryLocal(args);
			s.runQueries();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}
