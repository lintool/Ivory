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

package ivory.server;

import ivory.util.XMLTools;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class RunLocalRetrievalServer {

	private static final Logger sLogger = Logger.getLogger(RunLocalRetrievalServer.class);

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("usage: [config-file] [port]");
			System.exit(-1);
		}

		String configFile = args[0];

		FileSystem fs = FileSystem.getLocal(new Configuration());

		Document d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
				fs.open(new Path(configFile)));

		sLogger.info("Reading configuration...");
		NodeList servers = d.getElementsByTagName("server");

		if (servers.getLength() > 1)
			throw new Exception(
					"Error: multiple servers specified.  Cannot launch more than one server on local machine!");

		String sid = null;
		for (int i = 0; i < servers.getLength(); i++) {
			// get server id
			Node node = servers.item(i);
			sid = XMLTools.getAttributeValue(node, "id", null);
		}

		if (sid == null) {
			throw new Exception("Must specify a query id attribute for every server!");
		}

		int port = Integer.parseInt(args[1]);

		RetrievalServer server = new RetrievalServer();
		server.initialize(sid, configFile, fs);
		server.start(port);

		while (true)
			;
	}

}
