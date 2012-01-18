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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;


/**
 * A reader for processing delimited data files.  Typical usage pattern:
 * 
 * <pre>
 * DelimitedValuesFileReader iter = new DelimitedValuesFileReader("foo.txt");
 *     
 * String[] arr;
 * while ((arr = iter.nextValues()) != null) {
 *    String val1 = arr[0];
 *    String val2 = arr[1];
 *    // do something here
 * } 
 * </pre>
 */
public class DelimitedValuesFileReader {
	public static final String DEFAULT_DELIMITER = "\t";
	
	//private FileInputStream mStream;

	//private BufferedReader mReader;

	private FSDataInputStream in;

	private String mDelimiter;

	/**
	 * Constructs a <code>TabbedSeparatedValuesFileReader</code> with the
	 * contents of a file.
	 * 
	 * @param filename
	 *            name of file to read
	 */
	public DelimitedValuesFileReader(String filename) {
		this(filename, DEFAULT_DELIMITER);
	}

	public DelimitedValuesFileReader(String filename, String delimiter) {
		mDelimiter = delimiter;

		JobConf conf = new JobConf(DelimitedValuesFileReader.class);

		try {
			in = FileSystem.get(conf).open(new Path(filename));

			//mStream = new FileInputStream(new File(filename));
			//mReader = new BufferedReader(new InputStreamReader(mStream));
		} catch (Exception e) {
			throw new RuntimeException("Error: '" + filename + "' not found");
		}
	}

	/**
	 * Reads the next line.
	 * 
	 * @return an array with values from the next line, or <code>null</code>
	 *         if no more lines
	 */
	public String[] nextValues() {
		String line = null;

		try {
			//line = mReader.readLine();

			line = in.readLine();

		} catch (Exception e) {
			e.printStackTrace();
		}

		if (line == null || line.trim().equals(""))
			return null;

		return line.split(mDelimiter);
	}

	/**
	 * Closes file handles.
	 */
	public void destruct() {
		try {
			//mReader.close();
			//mStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
