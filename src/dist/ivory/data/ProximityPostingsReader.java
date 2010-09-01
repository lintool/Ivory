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
	protected ivory.data.PostingsReader[] _readers = null;

	/**
	 * size of ordered window
	 */
	protected int _size;

	/**
	 * keeps track of whether we're at the end of the individual term posting lists
	 */
	private boolean [] _endOfLists = null;

	/**
	 * are we at the end of this posting list?
	 */
	private boolean _endOfList = false;

	/**
	 * next matching posting 
	 */
	private Posting _nextPosting = null;

	/**
	 * current postings for each individual term
	 */
	private Posting [] _curPostings = null;

	public ProximityPostingsReader(ivory.data.PostingsReader[] readers, int size) {
		_readers = readers;
		_size = size;
		_initialize();
	}

	/**
	 * initializes the postings list reader
	 */
	private void _initialize() {
		_nextPosting = new Posting();

		_curPostings = new Posting[_readers.length];
		_endOfLists = new boolean[_readers.length];

		// get current postings for each term in window
		for(int i = 0; i < _readers.length; i++) {
			ivory.data.PostingsReader reader = _readers[i];			
			_curPostings[i] = new Posting();
			if(reader.nextPosting(_curPostings[i])) {
				_endOfLists[i] = false;
			}
			else {
				_endOfLists[i] = true;
			}
		}

		_endOfList = false;
		_findNextMatch();
	}

	/**
	 * finds the next matching window
	 */
	private void _findNextMatch() {
		int numEndOfLists = 0;
		while(numEndOfLists < _readers.length) {
			int target = -1;
			int nextTarget = -1;
			int matches = 0;
			for(int i = 0; i < _curPostings.length; i++) {
				if( i == 0 ) {
					target = _curPostings[0].getDocno();
					//System.err.println("Target: " + target);
				}

				// did we match our target docid?
				if(!_endOfLists[i] && _curPostings[i].getDocno() == target) {
					matches++;
				}

				// update our next target
				if( _curPostings[i].getDocno() > nextTarget ) {
					nextTarget = _curPostings[i].getDocno();
				}
				if( _endOfLists[i] ) {
					nextTarget = Integer.MAX_VALUE;
				}
			}

			//System.err.println("Matches: " + matches);
			if(matches == _curPostings.length) {
				_nextPosting.setDocno(target);
				_nextPosting.setScore(_countMatches());
				_advanceReaders(target+1);
				return;
			}
			else {
				_advanceReaders(nextTarget);
			}

			// count the number of lists
			numEndOfLists = 0;
			for(int i = 0; i < _readers.length; i++) {
				//System.err.println("_ofOfLists[" + i + "]: " + _endOfLists[i]);
				if(_endOfLists[i]) {
					numEndOfLists++;
				}
			}
			//System.err.println("end of lists: " + numEndOfLists);
		}

		_endOfList = true;
	}

	/**
	 * @param docno
	 */
	private void _advanceReaders(int docid) {
		//System.err.println("advancing readers to: " + docid);
		for(int i = 0; i < _readers.length; i++) {
			//System.err.println("_curPostings["+i+"].getDocno(): " + _curPostings[i].getDocno());
			ivory.data.PostingsReader reader = _readers[i];
			while(_curPostings[i].getDocno() < docid && !_endOfLists[i]) {
				if(reader.nextPosting(_curPostings[i])) {
					_endOfLists[i] = false;
				}
				else {
					_endOfLists[i] = true;
				}
			}
		}		
	}

	private short _countMatches() {
		int matches = 0;

		// merge all position lists into single stream
		List<TermPosition> allPositions = new ArrayList<TermPosition>();
		for(int i = 0; i < _readers.length; i++) {
			int [] positions = _readers[i].getPositions();
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

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#hasMorePostings()
	 */
	public boolean hasMorePostings() {
		return _endOfList;
	}

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#nextPosting(edu.umd.ivory.data.Posting)
	 */
	public boolean nextPosting(Posting posting) {
		posting.setDocno(_nextPosting.getDocno());
		posting.setScore(_nextPosting.getScore());
		_findNextMatch();
		return !hasMorePostings();
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

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.PostingsReader#reset()
	 */
	public void reset() {
		// reset posting list readers
		for(ivory.data.PostingsReader reader : _readers) {
			reader.reset();
		}

		// re-initialize the postings list reader
		_initialize();
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
