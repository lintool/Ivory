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

package ivory.core.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Don Metzler
 */
public class ResultWriter {
	// System-dependent newline character.
	protected static final String NEWLINE = System.getProperty("line.separator");
  protected static final int OUTPUT_BUFFER_SIZE = 1 * 1024 * 1024;

	protected Writer writer = null;
	protected GZIPOutputStream gzipStream = null;

	/**
	 * @param file
	 * @param compress
	 * @throws IOException
	 */
	public ResultWriter(String file, boolean compress, FileSystem fs) throws IOException {
		FSDataOutputStream out = fs.create(new Path(file), true);

		if (compress) {
			gzipStream = new GZIPOutputStream(out);
			writer = new OutputStreamWriter(gzipStream);
		} else {
			writer = new BufferedWriter(new OutputStreamWriter(out, "UTF8"), OUTPUT_BUFFER_SIZE);
		}
	}

	/**
	 * @param string
	 */
	public void println(String string) throws IOException {
		writer.write(string + NEWLINE);
	}

	/**
	 * @throws IOException
	 */
	public void flush() throws IOException {
		writer.flush();
		if (gzipStream != null) {
			gzipStream.finish();
		}
		writer.close();
	}
}
