//	metrics-influxdb
//
//	Written in 2014 by David Bernard <dbernard@novaquark.com>
//
//	[other author/contributor lines as appropriate]
//
//	To the extent possible under law, the author(s) have dedicated all copyright and
//	related and neighboring rights to this software to the public domain worldwide.
//	This software is distributed without any warranty.
//
//	You should have received a copy of the CC0 Public Domain Dedication along with
//	this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
package metrics_influxdb.v08;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * A reporter which publishes metric values to a InfluxDB server.
 *
 * @see <a href="http://influxdb.org/">InfluxDB - An open-source distributed
 *      time series database with no external dependencies.</a>
 */
public class ReporterV08 extends ScheduledReporter {
	private static String[] COLUMNS_TIMER = {
			"time", "count"
			, "min", "max", "mean", "std-dev"
			, "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
			, "one-minute", "five-minute", "fifteen-minute", "mean-rate"
			, "run-count"
	};
	private static String[] COLUMNS_HISTOGRAM = {
			"time", "count"
			, "min", "max", "mean", "std-dev"
			, "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
			, "run-count"
	};
	private static String[] COLUMNS_COUNT = {
			"time", "count"
	};
	private static String[] COLUMNS_GAUGE = {
			"time", "value"
	};
	private static String[] COLUMNS_METER = {
			"time", "count"
			, "one-minute", "five-minute", "fifteen-minute", "mean-rate"
	};

	static final Logger LOGGER = LoggerFactory.getLogger(ReporterV08.class);

	private final Influxdb influxdb;
	private final Clock clock;
	private final String prefix;
	// Optimization : use pointsXxx to reduce object creation, by reuse as arg of
	// Influxdb.appendSeries(...)
	private final Object[][] pointsTimer = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0l
	} };
	private final Object[][] pointsHistogram = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0l
	} };
	private final Object[][] pointsCounter = { {
		0l,
		0l
	} };
	private final Object[][] pointsGauge = { {
		0l,
		null
	} };
	private final Object[][] pointsMeter = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d
	} };

	protected final boolean skipIdleMetrics;
	protected final Map<String, Long> previousValues;

	public ReporterV08(MetricRegistry registry,
			Influxdb influxdb,
			Clock clock,
			String prefix,
			TimeUnit rateUnit,
			TimeUnit durationUnit,
			MetricFilter filter,
			boolean skipIdleMetrics,
			ScheduledExecutorService executor) {
		super(registry, "influxdb-reporter", filter, rateUnit, durationUnit, executor);
		this.skipIdleMetrics = skipIdleMetrics;
		this.previousValues = new TreeMap<String, Long>();
		this.influxdb = influxdb;
		this.clock = clock;
		this.prefix = (prefix == null) ? "" : (prefix.trim() + ".");
	}

	public ReporterV08(MetricRegistry registry,
			Influxdb influxdb,
			Clock clock,
			String prefix,
			TimeUnit rateUnit,
			TimeUnit durationUnit,
			MetricFilter filter,
			boolean skipIdleMetrics) {
		super(registry, "influxdb-reporter", filter, rateUnit, durationUnit);
		this.skipIdleMetrics = skipIdleMetrics;
		this.previousValues = new TreeMap<String, Long>();
		this.influxdb = influxdb;
		this.clock = clock;
		this.prefix = (prefix == null) ? "" : (prefix.trim() + ".");
	}

	/**
	 * Returns true if this metric is idle and should be skipped.
	 *
	 * @param name
	 * @param counting
	 * @return true if the metric should be skipped
	 */
	protected boolean canSkipMetric(String name, Counting counting) {
		boolean isIdle = calculateDelta(name, counting.getCount()) == 0L;
		if (skipIdleMetrics && !isIdle) {
			previousValues.put(name, counting.getCount());
		}
		return skipIdleMetrics && isIdle;
	}

	/**
	 * Calculate the delta from the current value to the previous reported value.
	 */
	private long calculateDelta(String name, long count) {
		Long previous = previousValues.get(name);
		if (previous == null) {
			// unknown metric, force non-zero delta to report
			return -1L;
		}
		if (count < previous) {
			LOGGER.warn("Saw a non-monotonically increasing value for metric '{}'", name);
			return 0L;
		}
		return count - previous;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void report(SortedMap<String, Gauge> gauges,
			SortedMap<String, Counter> counters,
			SortedMap<String, Histogram> histograms,
			SortedMap<String, Meter> meters,
			SortedMap<String, Timer> timers) {
		final long timestamp = clock.getTime();

		// oh it'd be lovely to use Java 7 here
		try {
			influxdb.resetRequest();

			for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
				reportGauge(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Counter> entry : counters.entrySet()) {
				reportCounter(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
				reportHistogram(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Meter> entry : meters.entrySet()) {
				reportMeter(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Timer> entry : timers.entrySet()) {
				reportTimer(entry.getKey(), entry.getValue(), timestamp);
			}

			if (influxdb.hasSeriesData()) {
				influxdb.sendRequest(true, false);
			}
		} catch (Exception e) {
			LOGGER.warn("Unable to report to InfluxDB. Discarding data.", e);
		}
	}

	private void reportTimer(String name, Timer timer, long timestamp) {
		if (canSkipMetric(name, timer)) {
			return;
		}
		final Snapshot snapshot = timer.getSnapshot();
		Object[] p = pointsTimer[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = snapshot.size();
		p[2] = convertDuration(snapshot.getMin());
		p[3] = convertDuration(snapshot.getMax());
		p[4] = convertDuration(snapshot.getMean());
		p[5] = convertDuration(snapshot.getStdDev());
		p[6] = convertDuration(snapshot.getMedian());
		p[7] = convertDuration(snapshot.get75thPercentile());
		p[8] = convertDuration(snapshot.get95thPercentile());
		p[9] = convertDuration(snapshot.get99thPercentile());
		p[10] = convertDuration(snapshot.get999thPercentile());
		p[11] = convertRate(timer.getOneMinuteRate());
		p[12] = convertRate(timer.getFiveMinuteRate());
		p[13] = convertRate(timer.getFifteenMinuteRate());
		p[14] = convertRate(timer.getMeanRate());
		p[15] = timer.getCount();
		assert (p.length == COLUMNS_TIMER.length);
		influxdb.appendSeries(prefix, name, ".timer", COLUMNS_TIMER, pointsTimer);
	}

	private void reportHistogram(String name, Histogram histogram, long timestamp) {
		if (canSkipMetric(name, histogram)) {
			return;
		}
		final Snapshot snapshot = histogram.getSnapshot();
		Object[] p = pointsHistogram[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = snapshot.size();
		p[2] = snapshot.getMin();
		p[3] = snapshot.getMax();
		p[4] = snapshot.getMean();
		p[5] = snapshot.getStdDev();
		p[6] = snapshot.getMedian();
		p[7] = snapshot.get75thPercentile();
		p[8] = snapshot.get95thPercentile();
		p[9] = snapshot.get99thPercentile();
		p[10] = snapshot.get999thPercentile();
		p[11] = histogram.getCount();
		assert (p.length == COLUMNS_HISTOGRAM.length);
		influxdb.appendSeries(prefix, name, ".histogram", COLUMNS_HISTOGRAM, pointsHistogram);
	}

	private void reportCounter(String name, Counter counter, long timestamp) {
		Object[] p = pointsCounter[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = counter.getCount();
		assert (p.length == COLUMNS_COUNT.length);
		influxdb.appendSeries(prefix, name, ".count", COLUMNS_COUNT, pointsCounter);
	}

	private void reportGauge(String name, Gauge<?> gauge, long timestamp) {
		Object[] p = pointsGauge[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = gauge.getValue();
		assert (p.length == COLUMNS_GAUGE.length);
		influxdb.appendSeries(prefix, name, ".value", COLUMNS_GAUGE, pointsGauge);
	}

	private void reportMeter(String name, Metered meter, long timestamp) {
		if (canSkipMetric(name, meter)) {
			return;
		}
		Object[] p = pointsMeter[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = meter.getCount();
		p[2] = convertRate(meter.getOneMinuteRate());
		p[3] = convertRate(meter.getFiveMinuteRate());
		p[4] = convertRate(meter.getFifteenMinuteRate());
		p[5] = convertRate(meter.getMeanRate());
		assert (p.length == COLUMNS_METER.length);
		influxdb.appendSeries(prefix, name, ".meter", COLUMNS_METER, pointsMeter);
	}
}
