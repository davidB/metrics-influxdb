package metrics_influxdb.measurements.reporter;

import com.codahale.metrics.Gauge;
import metrics_influxdb.measurements.Measure;

import java.util.Map;

public class GaugeMeasurementReporter {

  public Measure getMeasurement(String metricName, Map<String, String> tags, Gauge metric, long timestamp) {

    Measure measure = new Measure(metricName)
        .timestamp(timestamp)
        .addTag(tags);
    Object o = metric.getValue();

    if (o == null) {
      // skip null values
      return null;
    }

    if (o instanceof Long || o instanceof Integer) {
      long value = ((Number) o).longValue();
      measure.addValue("value", value);
    } else if (o instanceof Double) {
      Double d = (Double) o;
      if (d.isInfinite() || d.isNaN()) {
        // skip Infinite & NaN
        return null;
      }
      measure.addValue("value", d.doubleValue());
    } else if (o instanceof Float) {
      Float f = (Float) o;
      if (f.isInfinite() || f.isNaN()) {
        // skip Infinite & NaN
        return null;
      }
      measure.addValue("value", f.floatValue());
    } else {
      String value = "" + o;
      measure.addValue("value", value);
    }

    return measure;
  }
}
