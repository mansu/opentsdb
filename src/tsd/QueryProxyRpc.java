package net.opentsdb.tsd;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import org.jboss.netty.handler.codec.http.HttpMethod;

/**
 * Proxy handler for queries for timeseries datapoints.
 * It routes the query traffic to according downstream opentsdb-metron instances
 * gather the result and return to the client
 */
final class QueryProxyRpc implements HttpRpc {

  public QueryProxyRpc() {
  }

  /**
   * Implements the /api/query endpoint to route qurty to downstream opentsdb-metron instances
   * @param tsdb The TSDB to use for fetching data
   * @param query The HTTP query for parsing and responding
   */
  public void execute(final TSDB tsdb, final HttpQuery query)
    throws IOException {
    final String host = getHostByQuery(query);
    if (host == null) {
      return;
    }
    final String port = tsdb.getConfig().getString("tsd.network.port");
    final String uri = query.request().getUri();
    final URL url = new URL(String.format("http://%s:%s%s", host, port, uri));
    final HttpURLConnection con = (HttpURLConnection) url.openConnection();

    con.setRequestMethod(String.valueOf(query.method()));
    for (Map.Entry<String, String> entry: query.getHeaders().entrySet()) {
      con.setRequestProperty(entry.getKey(), entry.getValue());
    }
    if (query.method() == HttpMethod.POST) {
      con.setDoOutput(true);
      // Writing the post data to the HTTP request body
      BufferedWriter httpRequestBodyWriter = new BufferedWriter(new OutputStreamWriter(con.getOutputStream()));
      httpRequestBodyWriter.write(query.getContent());
      httpRequestBodyWriter.close();
    }

    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer content = new StringBuffer();
    while ((inputLine = in.readLine()) != null) {
      content.append(inputLine);
    }
    in.close();

    query.sendReply(String.valueOf(content).getBytes());
  }

  private String getHostByQuery(final HttpQuery query) {
    final TSQuery tsQuery = query.serializer().parseQueryV1();
    String host = null;
    for (TSSubQuery tsSubQuery : tsQuery.getQueries()) {
      String sampleMetric = tsSubQuery.getMetric();
      String currHost;
      if (sampleMetric.matches("ostrich.counters.serviceframework.followerservice.*")) {
        currHost = "cmp-metron-test-0a015435";
      } else {
        return null;
      }
      if (host == null) {
        host = currHost;
      } else if (host != currHost){
        // don't support pinging more than one host, so fail the query
        return null;
      }
    }
    return host;
  }
}
