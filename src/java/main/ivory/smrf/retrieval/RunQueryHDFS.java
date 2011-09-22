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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

@SuppressWarnings("deprecation")
public class RunQueryHDFS extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunQueryHDFS.class);
  private static enum Time { Query };

  private static class QueryRunner extends NullMapper {
    public void run(JobConf conf, Reporter reporter) throws IOException {
      String[] args = conf.get("args").split(";");
      FileSystem fs = FileSystem.get(conf);
      BatchQueryRunner qr;
      try {
        LOG.info("Initializing BatchQueryRunner...");
        qr = new BatchQueryRunner(args, fs);
        LOG.info("Running the queries ...");
        long start = System.currentTimeMillis();
        qr.runQueries();
        long end = System.currentTimeMillis();

        reporter.incrCounter(Time.Query, (end - start));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("usage: [queries-file] [models-file]");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String argsStr = Joiner.on(";").join(args);

    JobConf conf = new JobConf(getConf(), RunQueryHDFS.class);
    conf.setJobName("RunQueryHDFS");

    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);

    conf.setInputFormat(NullInputFormat.class);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapperClass(QueryRunner.class);

    conf.set("args", argsStr);
    conf.set("mapred.child.java.opts", "-Xmx16g");

    LOG.info("argsStr: " + argsStr);

    JobClient client = new JobClient(conf);
    client.submitJob(conf);

    LOG.info("runner started!");

    return 0;
  }

  public RunQueryHDFS() {}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunQueryHDFS(), args);
  }
}
