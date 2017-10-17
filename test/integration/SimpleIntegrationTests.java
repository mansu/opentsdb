package net.opentsdb.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.yuvi.chunk.ChunkManager;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SimpleIntegrationTests {

  private final static String configFile = "opentsdb_integration.conf";
  private Config config;
  private TSDB tsdb;

  @Test
  public void simpleTest1() {

    final TSQuery query = new TSQuery();
    query.setStart("1489647600");
    final TSSubQuery subQuery = new TSSubQuery();
    subQuery.setMetric("metric2");
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVFilter.Builder()
        .setType("literal_or")
        .setFilter("host1|host2")
        .setTagk("host")
        .setGroupBy(true)
        .build());
    subQuery.setFilters(filters);
    subQuery.setAggregator("sum");
    final ArrayList<TSSubQuery> subQueries = new ArrayList<TSSubQuery>(1);
    subQueries.add(subQuery);
    query.setQueries(subQueries);
    query.setMsResolution(false);

    /**
     Expected results
     metric2
     tags: cluster=cluster0 ec2_zone=us-east-1a host=host2 host_type=hosttype0
     aggregated tags: proc
     1489647610000 1020512.0
     1489647611000 1262148.0

     metric2
     tags: cluster=cluster0 ec2_zone=us-east-1c host=host1 host_type=hosttype0
     aggregated tags: proc
     1489647609000 2356225.0
     */

    Map<String, Map<String, String>> expectedTags = new HashMap<String, Map<String, String>>();
    expectedTags.put("host1", new HashMap<String, String>());
    expectedTags.get("host1").put("cluster", "cluster0");
    expectedTags.get("host1").put("ec2_zone", "us-east-1c");
    expectedTags.get("host1").put("host", "host1");
    expectedTags.get("host1").put("host_type", "hosttype0");
    expectedTags.put("host2", new HashMap<String, String>());
    expectedTags.get("host2").put("cluster", "cluster0");
    expectedTags.get("host2").put("ec2_zone", "us-east-1a");
    expectedTags.get("host2").put("host", "host2");
    expectedTags.get("host2").put("host_type", "hosttype0");
    Map<String, Set<String>> expectedAggregatedTags = new HashMap<String, Set<String>>();
    expectedAggregatedTags.put("host1", new HashSet<String>());
    expectedAggregatedTags.get("host1").add("proc");
    expectedAggregatedTags.put("host2", new HashSet<String>());
    expectedAggregatedTags.get("host2").add("proc");
    Map<String, List<TestDataPoint>> expectedDatapoints =
        new HashMap<String, List<TestDataPoint>>();
    expectedDatapoints.put("host1", new ArrayList<TestDataPoint>());
    expectedDatapoints.get("host1").add(new TestDataPoint(1489647609000l, 2356225.0));
    expectedDatapoints.put("host2", new ArrayList<TestDataPoint>());
    expectedDatapoints.get("host2").add(new TestDataPoint(1489647610000l, 1020512.0));
    expectedDatapoints.get("host2").add(new TestDataPoint(1489647611000l, 1262148.0));

    ArrayList<DataPoints[]> results = executeQuery(query);
    verify(results, expectedTags, expectedAggregatedTags, expectedDatapoints, "metric2");
  }

  @Test
  public void simpleTest2() {
    final TSQuery query = new TSQuery();
    query.setStart("1489647600");
    final TSSubQuery subQuery = new TSSubQuery();
    subQuery.setMetric("metric3");
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVFilter.Builder()
        .setType("wildcard")
        .setFilter("*")
        .setTagk("host")
        .setGroupBy(true)
        .build());
    subQuery.setFilters(filters);
    subQuery.setAggregator("zimsum");
    final ArrayList<TSSubQuery> subQueries = new ArrayList<TSSubQuery>(1);
    subQueries.add(subQuery);
    query.setQueries(subQueries);
    query.setMsResolution(false);

    /**
     Expected results
     metric3
     tags: cluster=cluster0 ec2_zone=us-east-1e host=host4 host_type=hosttype0
     aggregated tags:
     1489647608000 45056282.0

     metric3
     tags: cluster=cluster0 ec2_zone=us-east-1e host=host3 host_type=hosttype0
     aggregated tags:
     1489647610000 14263003.0
     */

    Map<String, Map<String, String>> expectedTags = new HashMap<String, Map<String, String>>();
    expectedTags.put("host4", new HashMap<String, String>());
    expectedTags.get("host4").put("cluster", "cluster0");
    expectedTags.get("host4").put("ec2_zone", "us-east-1e");
    expectedTags.get("host4").put("host", "host4");
    expectedTags.get("host4").put("host_type", "hosttype0");
    expectedTags.put("host3", new HashMap<String, String>());
    expectedTags.get("host3").put("cluster", "cluster0");
    expectedTags.get("host3").put("ec2_zone", "us-east-1e");
    expectedTags.get("host3").put("host", "host3");
    expectedTags.get("host3").put("host_type", "hosttype0");
    Map<String, Set<String>> expectedAggregatedTags = new HashMap<String, Set<String>>();
    expectedAggregatedTags.put("host4", new HashSet<String>());
    expectedAggregatedTags.put("host3", new HashSet<String>());
    Map<String, List<TestDataPoint>> expectedDatapoints =
        new HashMap<String, List<TestDataPoint>>();
    expectedDatapoints.put("host4", new ArrayList<TestDataPoint>());
    expectedDatapoints.get("host4").add(new TestDataPoint(1489647608000l, 45056282.0));
    expectedDatapoints.put("host3", new ArrayList<TestDataPoint>());
    expectedDatapoints.get("host3").add(new TestDataPoint(1489647610000l, 14263003.0));

    ArrayList<DataPoints[]> results = executeQuery(query);
    verify(results, expectedTags, expectedAggregatedTags, expectedDatapoints, "metric3");
  }

  private ArrayList<DataPoints[]> executeQuery(TSQuery query) {
    // make sure the query is valid. This will throw exceptions if something
    // is missing
    query.validateAndSetQuery();

    // compile the queries into TsdbQuery objects behind the scenes
    Query[] tsdbqueries = query.buildQueries(tsdb);

    // create some arrays for storing the results and the async calls
    final int nqueries = tsdbqueries.length;
    final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(
        nqueries);
    final ArrayList<Deferred<DataPoints[]>> deferreds =
        new ArrayList<Deferred<DataPoints[]>>(nqueries);

    // this executes each of the sub queries asynchronously and puts the
    // deferred in an array so we can wait for them to complete.
    for (int i = 0; i < nqueries; i++) {
      deferreds.add(tsdbqueries[i].runAsync());
    }

    // This is a required callback class to store the results after each
    // query has finished
    class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
      public Object call(final ArrayList<DataPoints[]> queryResults)
          throws Exception {
        results.addAll(queryResults);
        return null;
      }
    }

    // Make sure to handle any errors that might crop up
    class QueriesEB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception e) throws Exception {
        System.err.println("Queries failed");
        e.printStackTrace();
        return null;
      }
    }
    // this will cause the calling thread to wait until ALL of the queries
    // have completed.
    try {
      Deferred.groupInOrder(deferreds)
          .addCallback(new QueriesCB())
          .addErrback(new QueriesEB())
          .join();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return results;
  }

  private class TestDataPoint {
    public long timeStamp;
    public double value;
    public TestDataPoint(long timeStamp, double value) {
      this.timeStamp = timeStamp;
      this.value = value;
    }
  }

  private void verify(ArrayList<DataPoints[]> results,
                      Map<String, Map<String, String>> expectedTags,
                      Map<String, Set<String>> expectedAggregatedTags,
                      Map<String, List<TestDataPoint>> expectedDatapoints,
                      String metric) {


    assertEquals(1, results.size());
    assertEquals(2, results.get(0).length);
    for (final DataPoints data : results.get(0)) {
      assertEquals(metric, data.metricName());
      Map<String, String> resolvedTags = data.getTags();
      String host = resolvedTags.get("host");
      assertTrue(host != null);
      for (final Map.Entry<String, String> pair : resolvedTags.entrySet()) {
        assertEquals(expectedTags.get(host).get(pair.getKey()), pair.getValue());
      }
      assertEquals(expectedTags.get(host).size(), resolvedTags.size());
      List<String> aggregatedTags = data.getAggregatedTags();
      for (final String aggregatedTag : aggregatedTags) {
        assertTrue(expectedAggregatedTags.get(host).contains(aggregatedTag));
      }
      assertEquals(expectedAggregatedTags.get(host).size(), aggregatedTags.size());


      List<TestDataPoint> dataPoints = new ArrayList<TestDataPoint>();
      final SeekableView it = data.iterator();
      while (it.hasNext()) {
        DataPoint dp = it.next();
        dataPoints.add(new TestDataPoint(dp.timestamp(), dp.doubleValue()));
      }
      dataPoints.sort(new Comparator<TestDataPoint>() {
        @Override
        public int compare(TestDataPoint lhs, TestDataPoint rhs) {
          if (lhs.timeStamp < rhs.timeStamp) {
            return -1;
          }
          else if (lhs.timeStamp > rhs.timeStamp) {
            return 1;
          }
          else {
            return 0;
          }
        }
      });
      assertEquals(expectedDatapoints.get(host).size(), dataPoints.size());
      for (int i = 0; i < dataPoints.size(); i++) {
        assertEquals(expectedDatapoints.get(host).get(i).timeStamp, dataPoints.get(i).timeStamp);
        assertEquals(expectedDatapoints.get(host).get(i).value, dataPoints.get(i).value, 1e-8);
      }
    }
  }

  @Before
  public void initObjects() throws IOException {
    ChunkManager chunkManager = new ChunkManager("test", 100000);
    String[] data = new String[]{"put metric0 1489647603 305006 proc=proc0 host=host0 cluster=cluster0 host=host0 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric0 1489647603 126887 proc=proc1 host=host0 cluster=cluster0 host=host0 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric1 1489647603 1114284 proc=proc1 host=host0 cluster=cluster0 host=host0 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric2 1489647603 1114284 proc=proc1 host=host0 cluster=cluster0 host=host0 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric0 1489647603 159295 proc=proc2 host=host0 cluster=cluster0 host=host0 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric1 1489647603 53822604 proc=proc2 host=host0 cluster=cluster0 host=host0 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric2 1489647603 1223241 proc=proc2 host=host0 cluster=cluster0 host=host0 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric0 1489647603 210587 proc=proc3 host=host0 cluster=cluster0 host=host0 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric0 1489647609 541158 proc=proc0 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric0 1489647609 113744 proc=proc1 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric1 1489647609 1091024 proc=proc1 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric2 1489647609 1091024 proc=proc1 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric0 1489647609 165101 proc=proc2 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric1 1489647609 53557412 proc=proc2 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric2 1489647609 1217213 proc=proc2 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric0 1489647609 397534 proc=proc3 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric1 1489647609 47988 proc=proc3 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric2 1489647609 47988 proc=proc3 host=host1 cluster=cluster0 host=host1 host_type=hosttype0 ec2_zone=us-east-1c",
                                 "put metric0 1489647610 33188 proc=proc0 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric0 1489647610 96760 proc=proc1 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric1 1489647610 1020512 proc=proc1 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric2 1489647610 1020512 proc=proc1 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric0 1489647611 145246 proc=proc2 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric1 1489647611 53561760 proc=proc2 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric2 1489647611 1217312 proc=proc2 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric0 1489647611 22957 proc=proc3 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric1 1489647611 44836 proc=proc3 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric2 1489647611 44836 proc=proc3 host=host2 cluster=cluster0 host=host2 host_type=hosttype0 ec2_zone=us-east-1a",
                                 "put metric3 1489647608 45056282 host=host4 cluster=cluster0 host=host4 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric0 1489647609 171716 proc=proc0 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric1 1489647609 16988 proc=proc0 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric2 1489647609 8494 proc=proc0 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric0 1489647609 124674 proc=proc1 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric1 1489647609 1091072 proc=proc1 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric2 1489647609 1091072 proc=proc1 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric0 1489647609 148745 proc=proc2 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric1 1489647609 53021368 proc=proc2 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric2 1489647609 1205031 proc=proc2 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric0 1489647609 119447 proc=proc3 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric1 1489647609 39824 proc=proc3 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric2 1489647609 39824 proc=proc3 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e",
                                 "put metric3 1489647610 14263003 host=host3 cluster=cluster0 host=host3 host_type=hosttype0 ec2_zone=us-east-1e"};
    for (String str: data) {
      chunkManager.addMetric(str);
    }
    config = new Config(configFile);
    tsdb = new TSDB(null, config, chunkManager);
  }
}
