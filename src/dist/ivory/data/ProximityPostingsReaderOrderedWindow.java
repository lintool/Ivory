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

import java.util.BitSet;

/**
 * @author Don Metzler
 *
 */
public class ProximityPostingsReaderOrderedWindow extends ProximityPostingsReader {

	private int mNumReaders;
	private BitSet mMatchedIds = null;
	
	public ProximityPostingsReaderOrderedWindow(PostingsReader [] readers, int size) {
		super(readers, size);
		
		mNumReaders = readers.length;
		mMatchedIds = new BitSet(mNumReaders);
	}

	@Override
	public int countMatches(int [] positions, int [] ids) {
		int matches = 0;

		for( int i = 0; i < positions.length; i++ ) {
			int maxGap = 0;
			boolean ordered = true;
			
			mMatchedIds.clear();
			mMatchedIds.set(ids[i]);
			int matchedIDCounts = 1;
			
			int lastMatchedID = ids[i];
			int lastMatchedPos = positions[i];
			
			for( int j = i+1; j < positions.length; j++ ) {
				int curID = ids[j];
				int curPos = positions[j];
				if(!mMatchedIds.get(curID)) {
					mMatchedIds.set(curID);
					matchedIDCounts++;
					if( curID < lastMatchedID ) {
						ordered = false;
					}
					if( curPos - lastMatchedPos > maxGap ) {
						maxGap = curPos - lastMatchedPos;
					}
				}
				
				// stop looking if the maximum gap is too large
				// or the terms appear out of order
				if(maxGap > mSize || !ordered) {
					break;
				}
				
				// did we match all the terms, and in order?
				if(matchedIDCounts == mNumReaders && ordered) {
					matches++;
					break;
				}
			}
		}

		return matches;
	}
}
