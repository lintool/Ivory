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

package ivory.smrf.retrieval;

import java.util.Map;

/**
 * @author Lidan Wang
 *
 */
public interface QueryRunner_cascade extends QueryRunner{
	public Accumulator[] runQuery(String[] query);
	public void runQuery(String qid, String[] query);
	public Accumulator[] getResults(String qid);
	public Map<String, Accumulator[]> getResults();
	public void clearResults();
	public float[] getCascadeCostAllQueries();
	public float [] getCascadeCostAllQueries_lastStage();
}
