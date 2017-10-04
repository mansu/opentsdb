package net.opentsdb.core;

import com.pinterest.yuvi.writer.FileMetricWriter;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.chunk.QueryAggregation;
import com.pinterest.yuvi.chunk.ChunkManager;
import com.pinterest.yuvi.tagstore.Query;
import com.pinterest.yuvi.tagstore.TagMatcher;
import com.pinterest.yuvi.writer.MetricWriter;
import com.pinterest.yuvi.writer.kafka.KafkaMetricWriter;
import com.pinterest.yuvi.writer.kafka.MetricsWriterTask;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.filter.TagVNotLiteralOrFilter;
import net.opentsdb.query.filter.TagVRegexFilter;
import net.opentsdb.query.filter.TagVWildcardFilter;;
import net.opentsdb.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class YuviPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(YuviPlugin.class);

  private ChunkManager chunkManager;

  public YuviPlugin(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }

  public YuviPlugin(Config config) {
    chunkManager = new ChunkManager(
        config.getString("tsd.storage.yuvi.chunk_data_prefix"),
        config.getInt("tsd.storage.yuvi.expected_tag_store_size"));

    // Load metrics data from a file
    LOG.info("Starting Yuvi plugin. Loading data");
//    Path filePath = Paths.get(config.getString("tsd.storage.yuvi.sample_data"));
//    FileMetricWriter metricReader = new FileMetricWriter(filePath, chunkManager);
//    metricReader.start();

    final MetricWriter kafkaMetricsReader = new KafkaMetricWriter(chunkManager,
        config.getString("tsd.storage.yuvi.kafka_topic_name"),
        config.getString("tsd.storage.yuvi.kafka_boostrap_servers"),
        config.getString("tsd.storage.yuvi.kafka_client_groups"),
        config.getString("tsd.storage.yuvi.kafka_auto_commit"),
        config.getString("tsd.storage.yuvi.kafka_auto_commit.interval"),
        config.getString("tsd.storage.yuvi.kakfa_session_timeout"));
    final MetricsWriterTask metricsWriterTask = new MetricsWriterTask(kafkaMetricsReader);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(metricsWriterTask);
    LOG.info("Finished loading data. Yuvi is ready.");
  }

  public List<TimeSeries> read(long startTime, long endTime, String metricName, List<TagVFilter> filters) {
    List<TagMatcher> tagMatchers = new ArrayList<TagMatcher>();
    for (TagVFilter tagVFilter : filters) {
      System.out.println("tagVFilter.getType() = " + tagVFilter.getType());
      if (tagVFilter.getType().equals(TagVWildcardFilter.FILTER_NAME)) {
        TagVWildcardFilter tagVWildcardFilter = (TagVWildcardFilter)tagVFilter;
        if (tagVWildcardFilter.isCaseInsensitive()) {
          tagMatchers.add(TagMatcher.wildcardMatch(tagVFilter.getTagk(), "*"));
        }
        else {
          tagMatchers.add(TagMatcher.iwildcardMatch(tagVFilter.getTagk(), "*"));
        }
      }
      if (tagVFilter.getType().equals(TagVLiteralOrFilter.FILTER_NAME)) {
        StringBuffer sb = new StringBuffer();
        TagVLiteralOrFilter tagVLiteralOrFilter = (TagVLiteralOrFilter)tagVFilter;
        for (String str : tagVLiteralOrFilter.getLiterals()) {
          if (sb.length() == 0) {
            sb.append(str);
          }
          else {
            sb.append("|" + str);
          }
        }
        tagMatchers.add(TagMatcher.literalOrMatch(tagVLiteralOrFilter.getTagk(),
            sb.toString(), tagVLiteralOrFilter.get_case_insensitive()));
      }
      if (tagVFilter.getType().equals(TagVNotLiteralOrFilter.FILTER_NAME)) {
        StringBuffer sb = new StringBuffer();
        TagVNotLiteralOrFilter tagVNotLiteralOrFilter = (TagVNotLiteralOrFilter)tagVFilter;
        for (String str : tagVNotLiteralOrFilter.getLiterals()) {
          if (sb.length() == 0) {
            sb.append(str);
          }
          else {
            sb.append("|" + str);
          }
        }
        tagMatchers.add(TagMatcher.notLiteralOrMatch(tagVNotLiteralOrFilter.getTagk(),
            sb.toString(), tagVNotLiteralOrFilter.get_case_insensitive()));
      }
      if (tagVFilter.getType().equals(TagVRegexFilter.FILTER_NAME)) {
        TagVRegexFilter tagVRegexFilter = (TagVRegexFilter)tagVFilter;
        tagMatchers.add(TagMatcher.regExMatch(tagVRegexFilter.getTagk(),
            tagVRegexFilter.getFilter()));
      }
    }
    final Query YuviQuery = new Query(metricName, tagMatchers);
    return chunkManager.query(YuviQuery,
        startTime,
        endTime,
        QueryAggregation.NONE);
  }
}