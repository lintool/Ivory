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

import java.io.IOException;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.Indexable;

/**
 * Interface for a document forward index.
 * 
 * @author Jimmy Lin
 *
 * @param <T> type of document
 */
public interface DocumentForwardIndex<T extends Indexable> {

	public T getDocid(String docid);

	public T getDocno(int docno);

	public void loadIndex(DocnoMapping mapping, String file, String collection) throws IOException;
	
	public String getContentType();
}
