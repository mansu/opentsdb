package net.opentsdb.core;

import com.pinterest.yuvi.writer.FileMetricWriter;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.chunk.QueryAggregation;
import com.pinterest.yuvi.chunk.ChunkManager;
import com.pinterest.yuvi.tagstore.Query;
import com.pinterest.yuvi.tagstore.TagMatcher;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVWildcardFilter;
import net.opentsdb.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
    Path filePath = Paths.get(config.getString("tsd.storage.yuvi.sample_data"));
    FileMetricWriter metricReader = new FileMetricWriter(filePath, chunkManager);
    metricReader.start();
    LOG.info("Finished loading data. Yuvi is ready.");
  }

  public List<TimeSeries> read(long startTime, long endTime, String metricName, List<TagVFilter> filters) {
    List<TagMatcher> tagMatchers = new ArrayList<TagMatcher>();
    for (TagVFilter tagVFilter : filters) {
      if (tagVFilter.getType().equals(TagVWildcardFilter.FILTER_NAME)) {
        tagMatchers.add(TagMatcher.wildcardMatch(tagVFilter.getTagk(), "*"));
      }
    }
    final Query YuviQuery = new Query(metricName, tagMatchers);
    return chunkManager.query(YuviQuery,
        startTime,
        endTime,
        QueryAggregation.NONE);
  }
}