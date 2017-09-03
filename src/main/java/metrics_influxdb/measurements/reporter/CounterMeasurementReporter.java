package metrics_influxdb.measurements.reporter;

import com.codahale.metrics.Counter;
import metrics_influxdb.measurements.Measure;

import java.util.Map;

public class CounterMeasurementReporter {

	public Measure getMeasurement(String metricName, Map<String, String> tags, Counter metric, long timestamp) {

		return new Measure(metricName)
				.timestamp(timestamp)
				.addTag(tags)
				.addValue("count", metric.getCount());
	}
}
