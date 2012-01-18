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

package ivory.core.preprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PositionalSequenceFileRecordReader<K, V> extends RecordReader<K, V> {
  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  private K key = null;
  private V value = null;
  protected Configuration conf;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    conf = context.getConfiguration();
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new SequenceFile.Reader(fs, path, conf);
    this.end = fileSplit.getStart() + fileSplit.getLength();

    if (fileSplit.getStart() > in.getPosition()) {
      in.sync(fileSplit.getStart()); // sync to start
    }

    this.start = in.getPosition();
    more = start < end;
  }

  public long getPosition() throws IOException {
    return in.getPosition();
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!more) {
      return false;
    }
    long pos = in.getPosition();
    key = (K) in.next(key);
    if (key == null || (pos >= end && in.syncSeen())) {
      more = false;
      key = null;
      value = null;
    } else {
      value = (V) in.getCurrentValue(value);
    }
    return more;
  }

  @Override
  public K getCurrentKey() {
    return key;
  }

  @Override
  public V getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
    }
  }

  @Override
  public synchronized void close() throws IOException {
    in.close();
  }
}
