package net.opentsdb.core;

import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.TimeSeries;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.utils.Config;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class YuviPluginTest {

  @Test
  public void testRead() throws IOException {
    YuviPlugin YuviPlugin = new YuviPlugin(new Config(true));
    List<TimeSeries> timeSeriesList = YuviPlugin.read(1489647603, 1489647609,
        "metric0",
        new ArrayList<TagVFilter>());
    List<Point> expectedPoints = new ArrayList<Point>();

    expectedPoints.add(new Point(1489647609, 148745.0));
    expectedPoints.add(new Point(1489647610, 33188.0));
    expectedPoints.add(new Point(1489647609, 541158.0));
    expectedPoints.add(new Point(1489647603, 126887.0));
    expectedPoints.add(new Point(1489647603, 210587.0));
    expectedPoints.add(new Point(1489647609, 165101.0));
    expectedPoints.add(new Point(1489647609, 119447.0));
    expectedPoints.add(new Point(1489647611, 145246.0));
    expectedPoints.add(new Point(1489647609, 124674.0));
    expectedPoints.add(new Point(1489647609, 171716.0));
    expectedPoints.add(new Point(1489647610, 96760.0));
    expectedPoints.add(new Point(1489647609, 113744.0));
    expectedPoints.add(new Point(1489647611, 22957.0));
    expectedPoints.add(new Point(1489647609, 397534.0));
    expectedPoints.add(new Point(1489647603, 305006.0));
    expectedPoints.add(new Point(1489647603, 159295.0));

    Comparator<Point> cmp = new Comparator<Point>() {
      @Override
      public int compare(Point lhs, Point rhs) {
        if (lhs.getTs() < rhs.getTs()) {
          return -1;
        }
        else if (lhs.getTs() > rhs.getTs()) {
          return 1;
        }
        else if (lhs.getVal() < rhs.getVal()) {
          return -1;
        }
        else if (lhs.getVal() > rhs.getVal()) {
          return 1;
        }
        else {
          return 0;
        }
      }
    };
    expectedPoints.sort(cmp);
    List<Point> points = new ArrayList<Point>();

    for (int i = 0; i < timeSeriesList.size(); i++) {
      TimeSeries timeSeries = timeSeriesList.get(i);
      Assert.assertEquals(timeSeries.getPoints().size(), 1);
      Point point = timeSeries.getPoints().get(0);
      points.add(point);
    }
    points.sort(cmp);
    Assert.assertEquals(points.size(), expectedPoints.size());
    for (int i = 0; i < points.size(); i++) {
      Assert.assertEquals(points.get(i).getTs(), expectedPoints.get(i).getTs());
      Assert.assertEquals(points.get(i).getVal(), expectedPoints.get(i).getVal(), 1e-5);
    }
  }
}
