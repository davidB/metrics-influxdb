package metrics_influxdb.measurements.reporter;

import com.codahale.metrics.Meter;
import metrics_influxdb.measurements.Measure;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MeterMeasurementReporter {

	protected double rateFactor;

	public MeterMeasurementReporter(TimeUnit rateUnit) {
		this.rateFactor = rateUnit.toSeconds(1);
	}

	public Measure getMeasurement(String metricName, Map<String, String> tags, Meter metric, long timestamp) {

		Measure measure = new Measure(metricName)
				.timestamp(timestamp)
				.addTag(tags)
				.addValue("count", metric.getCount())
				.addValue("one-minute", convertRate(metric.getOneMinuteRate()))
				.addValue("five-minute", convertRate(metric.getFiveMinuteRate()))
				.addValue("fifteen-minute", convertRate(metric.getFifteenMinuteRate()))
				.addValue("mean-minute", convertRate(metric.getMeanRate()));
		return measure;
	}

	protected double convertRate(double rate) {
		return rate * rateFactor;
	}
}
