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
 */
public class ProximityPostingsListUnorderedWindow extends ProximityPostingsReader {

	/**
	 * @param readers
	 * @param size
	 */
	public ProximityPostingsListUnorderedWindow(PostingsReader[] readers, int size) {
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

			boolean [] matchedIDs = new boolean[_readers.length];
			matchedIDs[positions.get(i).id] = true;
			int matchedIDCounts = 1;
			
			int startPos = positions.get(i).position;
			//int lastMatchedID = allPositions.get(i).id;
			//int lastMatchedPos = allPositions.get(i).position;
			
			for( int j = i+1; j < positions.size(); j++ ) {
				int curID = positions.get(j).id;
				int curPos = positions.get(j).position;
				int windowSize = curPos - startPos + 1;
				if(!matchedIDs[curID]) {
					matchedIDs[curID] = true;
					matchedIDCounts++;
				}
				
				// stop looking if we've exceeded the maximum window size
				if(windowSize > _size ) {
					break;
				}
				
				// did we match all the terms?
				if(matchedIDCounts == _readers.length) {
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
