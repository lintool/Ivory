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

package ivory.http.server;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author Tamer Elsayed
 */
public class HomeServlet extends HttpServlet {
	private static final long serialVersionUID = 7368950575963429946L;

	private String mCollectionName = "";
	
	protected void doGet(HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse) throws ServletException, IOException {
		httpServletResponse.setContentType("text/html");
		PrintWriter out = httpServletResponse.getWriter();

		out.println("<html><head><title>Ivory Search Interface: " + mCollectionName + "</title><head>");
		out.println("<body>");
		out.println("<h3>Run a query on " + mCollectionName + ":</h3>");
		out.println("<form method=\"post\" action=\"" + DirectQueryServlet.ACTION + "\">");
		out.println("<input type=\"text\" name=\"" + DirectQueryServlet.QUERY_FIELD
				+ "\" size=\"60\" />");
		out.println("<input type=\"submit\" value=\"Run query!\" />");
		out.println("</form>");
		out.println("</p>");

		out.print("</body></html>\n");

		out.close();
	}
	
	public void setCollectionName(String name) {
		mCollectionName = name;
	}
	
	public String getCollectionName() {
		return mCollectionName;
	}
}