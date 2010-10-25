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
 */
public class ProximityPostingsReaderUnorderedWindow extends ProximityPostingsReader {

	private int mNumReaders;
	private BitSet mMatchedIds = null;

	public ProximityPostingsReaderUnorderedWindow(PostingsReader[] readers, int size) {
		super(readers, size);

		mNumReaders = readers.length;
		mMatchedIds = new BitSet(mNumReaders);
	}

	@Override
	public int countMatches(int [] positions, int [] ids) {
		int matches = 0;

		for( int i = 0; i < positions.length; i++ ) {
			mMatchedIds.clear();
			mMatchedIds.set(ids[i]);
			int matchedIDCounts = 1;
			
			int startPos = positions[i];
			
			for( int j = i+1; j < positions.length; j++ ) {
				int curID = ids[j];
				int curPos = positions[j];
				int windowSize = curPos - startPos + 1;
				if(!mMatchedIds.get(curID)) {
					mMatchedIds.set(curID);
					matchedIDCounts++;
				}
				
				// stop looking if we've exceeded the maximum window size
				if(windowSize > mSize ) {
					break;
				}
				
				// did we match all the terms?
				if(matchedIDCounts == mNumReaders) {
					matches++;
					break;
				}
			}
		}

		return matches;
	}
}
