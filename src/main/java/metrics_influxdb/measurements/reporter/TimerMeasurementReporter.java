package metrics_influxdb.measurements.reporter;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import metrics_influxdb.measurements.Measure;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TimerMeasurementReporter {

  protected double durationFactor;
  protected double rateFactor;
  protected boolean skipIdleMetrics;
  protected Map<String, Long> timerCallCounts;

  public TimerMeasurementReporter(TimeUnit rateUnit, TimeUnit durationUnit, boolean skipIdleMetrics) {
    this.rateFactor = rateUnit.toSeconds(1);
    this.durationFactor = 1.0 / durationUnit.toNanos(1);
    this.skipIdleMetrics = skipIdleMetrics;
    this.timerCallCounts = new ConcurrentHashMap<>();
  }

  public Measure getMeasurement(String metricName, String transformedName, Map<String, String> tags, Timer metric, long timestamp) {

    Snapshot snapshot = metric.getSnapshot();

    Measure measure = new Measure(transformedName)
        .timestamp(timestamp)
        .addTag(tags)
        .addValue("one-minute", convertRate(metric.getOneMinuteRate()))
        .addValue("five-minute", convertRate(metric.getFiveMinuteRate()))
        .addValue("fifteen-minute", convertRate(metric.getFifteenMinuteRate()))
        .addValue("mean-minute", convertRate(metric.getMeanRate()))
        .addValue("run-count", metric.getCount());

    if (!canSkip(metricName, metric)) {
      measure.addValue("count", snapshot.size())
          .addValue("min", convertDuration(snapshot.getMin()))
          .addValue("max", convertDuration(snapshot.getMax()))
          .addValue("mean", convertDuration(snapshot.getMean()))
          .addValue("std-dev", convertDuration(snapshot.getStdDev()))
          .addValue("50-percentile", convertDuration(snapshot.getMedian()))
          .addValue("75-percentile", convertDuration(snapshot.get75thPercentile()))
          .addValue("95-percentile", convertDuration(snapshot.get95thPercentile()))
          .addValue("99-percentile", convertDuration(snapshot.get99thPercentile()))
          .addValue("999-percentile", convertDuration(snapshot.get999thPercentile()));
    }

    return measure;
  }

  protected boolean canSkip(String metricName, Timer metric) {

    if (!skipIdleMetrics) {
      return false;
    }

    Long lastCount = timerCallCounts.get(metricName);
    Long currentCount = metric.getCount();
    timerCallCounts.put(metricName, lastCount);

    return lastCount == null || currentCount.equals(lastCount);
  }

  protected double convertDuration(double duration) {
    return duration * durationFactor;
  }

  protected double convertRate(double rate) {
    return rate * rateFactor;
  }
}
