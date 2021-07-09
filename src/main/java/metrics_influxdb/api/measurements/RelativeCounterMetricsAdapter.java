package metrics_influxdb.api.measurements;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import metrics_influxdb.measurements.Measure;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Paul Klingelhuber
 */
public class RelativeCounterMetricsAdapter implements MetricsAdapter {

  private final Map<String, Long> previousCounts = new HashMap<>();

  @Override
  public Measure adapt(String name, Metric metric, Measure measure) {
    if (metric instanceof Counter) {
      final Counter counter = (Counter) metric;
      final long count = counter.getCount();
      long previous = getPreviousCountAndSaveCurrent(name, count);
      // add count again to overwrite any previously set one (could have changed in the meantime)
      measure.addValue("count", count);
      measure.addValue("relativeCount", count - previous);
    }
    return measure;
  }

  private long getPreviousCountAndSaveCurrent(String name, long count) {
    final Long previous = previousCounts.get(name);
    previousCounts.put(name, count);
    if (previous == null) {
      return 0;
    }
    return previous;
  }

}
