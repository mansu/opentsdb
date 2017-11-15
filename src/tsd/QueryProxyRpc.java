package net.opentsdb.tsd;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * Proxy handler for queries for timeseries datapoints.
 * It routes the query traffic to according downstream opentsdb-metron instances
 * gather the result and return to the client
 */
final class QueryProxyRpc implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(QueryProxyRpc.class);

  private static final String proxyConfigFilePath = "config/yuvi_services.json";

  private static JSONObject yuviService = null;

  private static String proxyConfig;

  static {
    try {
      yuviService = (JSONObject)new JSONParser().parse(new FileReader(proxyConfigFilePath));
      LOG.info("Loaded config file {} with contents: {}", proxyConfigFilePath, yuviService);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

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
      String currHost = null;
      for(Object key : yuviService.keySet()) {
        if (tsSubQuery.getMetric().matches((String)key)) {
          currHost = yuviService.get(key).toString();
          break;
        }
      }

      if (currHost == null) {
        // can't find a match, return null and fail the query
        return null;
      } else if (host == null) {
        // keep track of the host for the return value
        host = currHost;
      } else if (host != currHost){
        // don't support pinging more than one host, so return null and fail the query
        return null;
      }
    }
    return host;
  }
}
