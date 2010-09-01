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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author Don Metzler
 *
 */
public abstract class ProximityPostingsReader implements ivory.data.PostingsReader {	
	/**
	 * readers for terms that make up ordered window
	 */
	protected ivory.data.PostingsListDocSortedPositional.PostingsReader [] mReaders = null;

	/**
	 * size of ordered window
	 */
	protected int mSize;

	public ProximityPostingsReader(ivory.data.PostingsListDocSortedPositional.PostingsReader[] readers, int size) {
		mReaders = readers;
		mSize = size;
	}

	/**
	 * @return true if current reader configuration represents a match
	 */
	private boolean isMatching() {
		int target = -1;
		for(ivory.data.PostingsListDocSortedPositional.PostingsReader reader : mReaders) {
			if(target == -1) {
				target = reader.getDocno();
			}

			// did we match our target docid?
			if(reader.getDocno() != target) {
				return false;
			}
		}

		return true;
	}

	private short countMatches() {
		int matches = 0;

		// merge all position lists into single stream
		List<TermPosition> allPositions = new ArrayList<TermPosition>();
		for(int i = 0; i < mReaders.length; i++) {
			int [] positions = mReaders[i].getPositions();
			for(int j = 0; j < positions.length; j++ ) {
				allPositions.add(new TermPosition(i, positions[j]));
			}
		}
		Collections.sort(allPositions, new TermPositionComparator());

		// count matches
		matches = countMatches(allPositions);

		// truncate tf to Short.MAX_VALUE
		if( matches > Short.MAX_VALUE ) {
			matches = Short.MAX_VALUE;
		}

		return (short)matches;
	}

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#getPositions()
	 */
	public int[] getPositions() {
		throw new UnsupportedOperationException();
	}

	public boolean getPositions(TermPositions tp){
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#hasMorePostings()
	 */
	public boolean hasMorePostings() {
		for(ivory.data.PostingsListDocSortedPositional.PostingsReader reader : mReaders) {
			if(!reader.hasMorePostings()) {
				return false;
			}
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#nextPosting(edu.umd.ivory.data.Posting)
	 */
	public boolean nextPosting(Posting posting) {
		// advance the reader at the minimum docno
		ivory.data.PostingsListDocSortedPositional.PostingsReader minReader = null;
		ivory.data.PostingsListDocSortedPositional.PostingsReader maxReader = null;
		for(ivory.data.PostingsListDocSortedPositional.PostingsReader reader : mReaders) {
			int docno = reader.getDocno();
			if(minReader == null || docno < minReader.getDocno()) {
				minReader = reader;
			}
			if(maxReader == null || docno > maxReader.getDocno()) {
				maxReader = reader;
			}
		}
		if(minReader != null && minReader.hasMorePostings() && minReader.nextPosting(posting)) {
			if(posting.getDocno() > maxReader.getDocno()) {
				maxReader = minReader;
			}
		}
		else {
			return false;
		}

		// docno = max( mReaders.getDocno() )
		posting.setDocno(maxReader.getDocno());
		if(isMatching()) {
			posting.setScore(countMatches());
		}
		else {
			posting.setScore((short)0);
		}

		return true;
	}

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#peekNextDocno()
	 */
	public int peekNextDocno() {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#peekNextScore()
	 */
	public short peekNextScore() {
		throw new UnsupportedOperationException();
	}

	public int getDocno() {
		int maxDocno = Integer.MIN_VALUE;
		for(ivory.data.PostingsListDocSortedPositional.PostingsReader reader : mReaders) {
			int docno = reader.getDocno();
			if(docno > maxDocno) {
				maxDocno = docno;
			}
		}
		//System.err.println("[proximity] getDocno() = " + maxDocno);
		return maxDocno;
	}
	
	public short getScore() {
		if(isMatching()) {
			return countMatches();
		}
		return 0;
	}
	
	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#reset()
	 */
	public void reset() {
		// reset posting list readers
		for(ivory.data.PostingsListDocSortedPositional.PostingsReader reader : mReaders) {
			reader.reset();
		}
	}

	public int getNumberOfPostings() {
		throw new UnsupportedOperationException();
	}

	public PostingsList getPostingsList() {
		throw new UnsupportedOperationException();
	}

	/**
	 * @author metzler
	 *
	 */
	public class TermPosition {
		public int position;
		public int id;

		/**
		 * @param id
		 * @param position
		 */
		public TermPosition(int id, int position) {
			this.id = id;
			this.position = position;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			return "position: " + position + ", id: " + id;
		}
	}

	/**
	 * @author metzler
	 *
	 */
	public class TermPositionComparator implements Comparator<TermPosition> {

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		public int compare(TermPosition o1, TermPosition o2) {
			if( o1.position < o2.position )
				return -1;
			else if( o1.position > o2.position )
				return 1;
			else
				return 0;			
		}

	}

	public abstract int countMatches(List<TermPosition> positions);
}
