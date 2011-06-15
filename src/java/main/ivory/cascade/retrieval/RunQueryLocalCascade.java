/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.cascade.retrieval;

import java.io.IOException;
import java.rmi.NotBoundException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

/**
 * @author Lidan Wang
 */
public class RunQueryLocalCascade {
  private static final Logger LOG = Logger.getLogger(RunQueryLocalCascade.class);

  private CascadeBatchQueryRunner runner = null;

  public RunQueryLocalCascade(String[] args) throws SAXException, IOException,
      ParserConfigurationException, Exception, NotBoundException {
    LOG.info("Initializing QueryRunner...");

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    runner = new CascadeBatchQueryRunner(args, fs);
  }

  /**
   * runs the queries
   */
  public void runQueries() throws Exception {
    LOG.info("Running queries ...");
    long start = System.currentTimeMillis();
    runner.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");
  }

  public static void main(String[] args) throws Exception {
    new RunQueryLocalCascade(args).runQueries();
    System.exit(0);
  }
}
