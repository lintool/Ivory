package ivory.core.data.stat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

public interface DocScoreTable {

	public void initialize(String file, FileSystem fs) throws IOException;

	/**
	 * Returns the length of a document.
	 */
	public float getScore(int docno);

	public int getDocnoOffset();

	/**
	 * Returns number of documents in the collection.
	 */
	public int getDocCount();

}
