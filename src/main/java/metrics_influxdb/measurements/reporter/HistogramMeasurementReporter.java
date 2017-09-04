package metrics_influxdb.measurements.reporter;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import metrics_influxdb.measurements.Measure;

import java.util.Map;

public class HistogramMeasurementReporter {

	public Measure getMeasurement(String metricName, Map<String, String> tags, Histogram metric, long timestamp) {

		Snapshot snapshot = metric.getSnapshot();

		return new Measure(metricName)
				.timestamp(timestamp)
				.addTag(tags)
				.addValue("count", snapshot.size())
				.addValue("min", snapshot.getMin())
				.addValue("max", snapshot.getMax())
				.addValue("mean", snapshot.getMean())
				.addValue("std-dev", snapshot.getStdDev())
				.addValue("50-percentile", snapshot.getMedian())
				.addValue("75-percentile", snapshot.get75thPercentile())
				.addValue("95-percentile", snapshot.get95thPercentile())
        .addValue("99-percentile", snapshot.get99thPercentile())
				.addValue("999-percentile", snapshot.get999thPercentile())
				.addValue("run-count", metric.getCount());
	}
}
