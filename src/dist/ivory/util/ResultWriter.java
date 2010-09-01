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

package ivory.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

/**
 * @author Don Metzler
 *
 */
public class ResultWriter {

	/**
	 * output buffer size 
	 */
	private static final int OUTPUT_BUFFER_SIZE = 1 * 1024*1024;

	/**
	 * system-dependent newline character 
	 */
	private static final String newLine = System.getProperty("line.separator");
	
	/**
	 * print writer
	 */
	private Writer mWriter = null;
	
	/**
	 * gzip output stream 
	 */
	private GZIPOutputStream mGzipStream = null;

	/**
	 * @param file
	 * @param compress
	 * @throws IOException
	 */
	public ResultWriter(String file, boolean compress) throws IOException {
		OutputStream out = null;
		
		if( file == null ) {
			out = System.out;
		} else {
			out = new FileOutputStream( file );
		}
		
		if( compress ) {
			mGzipStream = new GZIPOutputStream( out );
			mWriter = new OutputStreamWriter( mGzipStream );
		} else {
			mWriter = new BufferedWriter( new OutputStreamWriter( out ), OUTPUT_BUFFER_SIZE );
		}
	}

	/**
	 * @param string
	 */
	public void println(String string) throws IOException {
		
		mWriter.write( string + newLine );
	}

	/**
	 * @throws IOException
	 */
	public void flush() throws IOException {
		mWriter.flush();
		if( mGzipStream != null ) {
			mGzipStream.finish();
		}
		mWriter.close();
	}
}
