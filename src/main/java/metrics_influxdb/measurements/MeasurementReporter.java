package metrics_influxdb.measurements;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import metrics_influxdb.api.measurements.MetricMeasurementTransformer;
import metrics_influxdb.measurements.reporter.*;

public class MeasurementReporter extends ScheduledReporter{

  private final Sender sender;
	private final Clock clock;
	private Map<String, String> baseTags;
	private MetricMeasurementTransformer transformer;

	private GaugeMeasurementReporter gaugeMeasurementReporter;
	private HistogramMeasurementReporter histogramMeasurementReporter;
	private MeterMeasurementReporter meterMeasurementReporter;
	private TimerMeasurementReporter timerMeasurementReporter;
	private CounterMeasurementReporter counterMeasurementReporter;

	public MeasurementReporter(Sender sender, MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, Clock clock, Map<String, String> baseTags, MetricMeasurementTransformer transformer, CounterMeasurementReporter counterMeasurementReporter, GaugeMeasurementReporter gaugeMeasurementReporter, HistogramMeasurementReporter histogramMeasurementReporter, MeterMeasurementReporter meterMeasurementReporter, TimerMeasurementReporter timerMeasurementReporter, ScheduledExecutorService executor) {
		super(registry, "measurement-reporter", filter, rateUnit, durationUnit, executor);
		this.baseTags = baseTags;
		this.sender = sender;
		this.clock = clock;
		this.transformer = transformer;
		this.counterMeasurementReporter = counterMeasurementReporter;
		this.gaugeMeasurementReporter = gaugeMeasurementReporter;
		this.histogramMeasurementReporter = histogramMeasurementReporter;
		this.meterMeasurementReporter = meterMeasurementReporter;
		this.timerMeasurementReporter = timerMeasurementReporter;
	}

	public MeasurementReporter(Sender sender, MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, Clock clock, Map<String, String> baseTags, MetricMeasurementTransformer transformer, CounterMeasurementReporter counterMeasurementReporter, GaugeMeasurementReporter gaugeMeasurementReporter, HistogramMeasurementReporter histogramMeasurementReporter, MeterMeasurementReporter meterMeasurementReporter, TimerMeasurementReporter timerMeasurementReporter) {
		super(registry, "measurement-reporter", filter, rateUnit, durationUnit);
		this.baseTags = baseTags;
		this.sender = sender;
		this.clock = clock;
		this.transformer = transformer;
		this.counterMeasurementReporter = counterMeasurementReporter;
		this.gaugeMeasurementReporter = gaugeMeasurementReporter;
		this.histogramMeasurementReporter = histogramMeasurementReporter;
		this.meterMeasurementReporter = meterMeasurementReporter;
		this.timerMeasurementReporter = timerMeasurementReporter;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void report(SortedMap<String, Gauge> gauges
			, SortedMap<String, Counter> counters
			, SortedMap<String, Histogram> histograms
			, SortedMap<String, Meter> meters
			, SortedMap<String, Timer> timers) {

		final long timestamp = clock.getTime();

		for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {

			String metricName = transformer.measurementName(entry.getKey());
			Map<String, String> tags = new HashMap<>(baseTags);
			tags.putAll(transformer.tags(entry.getKey()));

			sender.send(gaugeMeasurementReporter.getMeasurement(metricName, tags, entry.getValue(), timestamp));
		}

		for (Map.Entry<String, Counter> entry : counters.entrySet()) {

			String metricName = transformer.measurementName(entry.getKey());
			Map<String, String> tags = new HashMap<>(baseTags);
			tags.putAll(transformer.tags(entry.getKey()));

			sender.send(counterMeasurementReporter.getMeasurement(metricName, tags, entry.getValue(), timestamp));
		}

		for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {

			String metricName = transformer.measurementName(entry.getKey());
			Map<String, String> tags = new HashMap<>(baseTags);
			tags.putAll(transformer.tags(entry.getKey()));

			sender.send(histogramMeasurementReporter.getMeasurement(metricName, tags, entry.getValue(), timestamp));
		}

		for (Map.Entry<String, Meter> entry : meters.entrySet()) {

			String metricName = transformer.measurementName(entry.getKey());
			Map<String, String> tags = new HashMap<>(baseTags);
			tags.putAll(transformer.tags(entry.getKey()));

			sender.send(meterMeasurementReporter.getMeasurement(metricName, tags, entry.getValue(), timestamp));
		}

		for (Map.Entry<String, Timer> entry : timers.entrySet()) {

			String metricName = transformer.measurementName(entry.getKey());
			Map<String, String> tags = new HashMap<>(baseTags);
			tags.putAll(transformer.tags(entry.getKey()));

			sender.send(timerMeasurementReporter.getMeasurement(metricName, tags, entry.getValue(), timestamp));
		}

		sender.flush();
	}
}
