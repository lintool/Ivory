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

package ivory.core.data.index;

import java.util.Arrays;
import java.util.BitSet;

/**
 * @author Don Metzler
 */
public class ProximityPostingsReaderOrderedWindow extends ProximityPostingsReader {
  private static final int BUFFER_SIZE = 4096;

  protected int numReaders;
  protected BitSet matchedIds = null;
  protected int[] newPositions = new int[BUFFER_SIZE];
  protected int[] newIds = new int[BUFFER_SIZE];

  public ProximityPostingsReaderOrderedWindow(PostingsReader[] readers, int size) {
    super(readers, size);

    numReaders = readers.length;
    matchedIds = new BitSet(numReaders);
  }

  @Override
  protected short countMatches() {
    int matches = 0;

    // Merge all position lists into single stream.
    int[] positions = readers[0].getPositions();
    int[] ids = new int[positions.length];
    Arrays.fill(ids, 0);
    int length = positions.length;

    for (int id = 1; id < readers.length; id++) {
      int[] p = readers[id].getPositions();

      if (length + p.length > newPositions.length) {
        newPositions = new int[length + p.length];
        newIds = new int[length + p.length];
      }

      int posA = 0;
      int posB = 0;
      int i = 0;
      while (i < length + p.length) {
        if (posB == p.length || (posA < length && positions[posA] <= p[posB])) {
          newPositions[i] = positions[posA];
          newIds[i] = ids[posA];
          posA++;
        } else {
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

    // Count matches.
    matches = countMatches(positions, ids);

    // Truncate tf to Short.MAX_VALUE.
    if (matches > Short.MAX_VALUE) {
      matches = Short.MAX_VALUE;
    }

    return (short) matches;
  }

  @Override
  protected int countMatches(int[] positions, int[] ids) {
    int matches = 0;

    for (int i = 0; i < positions.length; i++) {
      int maxGap = 0;
      boolean ordered = true;

      matchedIds.clear();
      matchedIds.set(ids[i]);
      int matchedIDCounts = 1;

      int lastMatchedID = ids[i];
      int lastMatchedPos = positions[i];

      for (int j = i + 1; j < positions.length; j++) {
        int curID = ids[j];
        int curPos = positions[j];
        if (!matchedIds.get(curID)) {
          matchedIds.set(curID);
          matchedIDCounts++;
          if (curID < lastMatchedID) {
            ordered = false;
          }
          if (curPos - lastMatchedPos > maxGap) {
            maxGap = curPos - lastMatchedPos;
          }

          lastMatchedPos = curPos;
          lastMatchedID = curID;
        }

        // Stop looking if the maximum gap is too large or the terms appear out of order.
        if (maxGap > size || !ordered) {
          break;
        }

        // Did we match all the terms, and in order?
        if (matchedIDCounts == numReaders && ordered) {
          matches++;
          break;
        }
      }
    }

    return matches;
  }
}
