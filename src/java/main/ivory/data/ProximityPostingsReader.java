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

package ivory.data;

import ivory.index.TermPositions;

import java.util.Arrays;

/**
 * @author Don Metzler
 * 
 */
public abstract class ProximityPostingsReader implements PostingsReader {

	

	/**
	 * readers for terms that make up ordered window
	 */
	protected PostingsReader[] mReaders = null;

	//protected int [] newPositions = new int[BUFFER_SIZE];
	//protected int [] newIds = new int[BUFFER_SIZE];

	/**
	 * size of ordered window
	 */
	protected int mSize;

	public ProximityPostingsReader(PostingsReader[] readers, int size) {
		mReaders = readers;
		mSize = size;
	}

	/**
	 * @return true if current reader configuration represents a match
	 */
	private boolean isMatching() {
		int target = -1;
		for (PostingsReader reader : mReaders) {
			if (target == -1) {
				target = reader.getDocno();
			}

			// did we match our target docid?
			if (reader.getDocno() != target) {
				return false;
			}
		}

		return true;
	}

	abstract protected short countMatches();
	/*{
		int matches = 0;

		// merge all position lists into single stream
		int[] positions = mReaders[0].getPositions();
		int[] ids = new int[positions.length];
		Arrays.fill(ids, 0);
		int length = positions.length;

		for (int id = 1; id < mReaders.length; id++) {
			int [] p = mReaders[id].getPositions();

			if(length + p.length > newPositions.length) {
				newPositions = new int[length + p.length];
				newIds = new int[length + p.length];
			}

			int posA = 0;
			int posB = 0;
			int i = 0;
			while(i < length + p.length) {
				if(posB == p.length || (posA < length && positions[posA] <= p[posB])) {
					newPositions[i] = positions[posA];
					newIds[i] = ids[posA];
					posA++;
				}
				else {
					newPositions[i] = p[posB];
					newIds[i] = id;
					posB++;
				}
				i++;
			}

			length += p.length;
			positions = Arrays.copyOf(newPositions, length);
			ids = Arrays.copyOf(newIds, length);
		}

		// count matches
		matches = countMatches(positions, ids);

		// truncate tf to Short.MAX_VALUE
		if (matches > Short.MAX_VALUE) {
			matches = Short.MAX_VALUE;
		}

		return (short) matches;
	}*/

	public int[] getPositions() {
		throw new UnsupportedOperationException();
	}

	public boolean getPositions(TermPositions tp) {
		throw new UnsupportedOperationException();
	}

	public byte[] getBytePositions(){
		throw new UnsupportedOperationException();
	}
	
	public boolean hasMorePostings() {
		for (PostingsReader reader : mReaders) {
			if (!reader.hasMorePostings()) {
				return false;
			}
		}
		return true;
	}

	public boolean nextPosting(Posting posting) {
		// advance the reader at the minimum docno
		PostingsReader minReader = null;
		PostingsReader maxReader = null;
		for (PostingsReader reader : mReaders) {
			int docno = reader.getDocno();
			//-------
			/*if(docno<=0){
				reader.nextPosting(posting);
				docno = reader.getDocno();	
			}*/
			//-------
			if (minReader == null || docno < minReader.getDocno()) {
				minReader = reader;
			}
			if (maxReader == null || docno > maxReader.getDocno()) {
				maxReader = reader;
			}
		}
		if (minReader != null && minReader.hasMorePostings() && minReader.nextPosting(posting)) {
			if (posting.getDocno() > maxReader.getDocno()) {
				maxReader = minReader;
			}
		} else {
			return false;
		}

		posting.setDocno(maxReader.getDocno());
		if (isMatching()) {
			posting.setScore(countMatches());
		} else {
			posting.setScore((short) 0);
		}

		return true;
	}

	public int peekNextDocno() {
		throw new UnsupportedOperationException();
	}

	public short peekNextScore() {
		throw new UnsupportedOperationException();
	}

	public int getDocno() {
		int maxDocno = Integer.MIN_VALUE;
		for (PostingsReader reader : mReaders) {
			int docno = reader.getDocno();
			if (docno > maxDocno) {
				maxDocno = docno;
			}
		}

		return maxDocno;
	}

	public short getScore() {
		if (isMatching()) {
			return countMatches();
		}
		return 0;
	}

	public void reset() {
		for (PostingsReader reader : mReaders) {
			reader.reset();
		}
	}

	public int getNumberOfPostings() {
		throw new UnsupportedOperationException();
	}

	public PostingsList getPostingsList() {
		throw new UnsupportedOperationException();
	}

	protected abstract int countMatches(int[] positions, int[] ids);
}
