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

import java.util.List;

/**
 * @author Don Metzler
 *
 */
public class ProximityPostingsListOrderedWindow extends ProximityPostingsReader {

	/**
	 * @param readers
	 * @param size
	 */
	public ProximityPostingsListOrderedWindow(ivory.data.PostingsListDocSortedPositional.PostingsReader [] readers, int size) {
		super(readers, size);
	}

	/* (non-Javadoc)
	 * @see edu.umd.ivory.data.ProximityPostingsList#countMatches(java.util.List)
	 */
	@Override
	public int countMatches(List<TermPosition> positions) {
		int matches = 0;
		//System.err.println("<positions>");
		for( int i = 0; i < positions.size(); i++ ) {
			//System.err.println(positions.get(i).toString());

			int maxGap = 0;
			boolean ordered = true;
			
			boolean [] matchedIDs = new boolean[mReaders.length];
			matchedIDs[positions.get(i).id] = true;
			int matchedIDCounts = 1;
			
			int lastMatchedID = positions.get(i).id;
			int lastMatchedPos = positions.get(i).position;
			
			for( int j = i+1; j < positions.size(); j++ ) {
				int curID = positions.get(j).id;
				int curPos = positions.get(j).position;
				if(!matchedIDs[curID]) {
					matchedIDs[curID] = true;
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
				if(matchedIDCounts == mReaders.length && ordered) {
					//System.out.println("MATCH");
					matches++;
					break;
				}
			}
		}
		//System.err.println("</positions>");
		return matches;
	}
}
