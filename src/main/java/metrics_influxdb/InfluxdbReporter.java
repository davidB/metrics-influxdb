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
package metrics_influxdb;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a InfluxDB server.
 *
 * @see <a href="http://influxdb.org/">InfluxDB - An open-source distributed
 *      time series database with no external dependencies.</a>
 */
public class InfluxdbReporter extends ScheduledReporter {
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

	/**
	 * Returns a new {@link Builder} for {@link InfluxdbReporter}.
	 *
	 * @param registry
	 *          the registry to report
	 * @return a {@link Builder} instance for a {@link InfluxdbReporter}
	 */
	public static Builder forRegistry(MetricRegistry registry) {
		return new Builder(registry);
	}

	/**
	 * A builder for {@link InfluxdbReporter} instances. Defaults to not using a
	 * prefix, using the default clock, converting rates to events/second,
	 * converting durations to milliseconds, and not filtering metrics.
	 */
	public static class Builder {
		private final MetricRegistry registry;
		private Clock clock;
		private String prefix;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;
		private MetricFilter filter;
		private boolean skipIdleMetrics;

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
		}

		/**
		 * Use the given {@link Clock} instance for the time.
		 *
		 * @param clock a {@link Clock} instance
		 * @return {@code this}
		 */
		public Builder withClock(Clock clock) {
			this.clock = clock;
			return this;
		}

		/**
		 * Prefix all metric names with the given string.
		 *
		 * @param prefix the prefix for all metric names
		 * @return {@code this}
		 */
		public Builder prefixedWith(String prefix) {
			this.prefix = prefix;
			return this;
		}

		/**
		 * Convert rates to the given time unit.
		 *
		 * @param rateUnit a unit of time
		 * @return {@code this}
		 */
		public Builder convertRatesTo(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		/**
		 * Convert durations to the given time unit.
		 *
		 * @param durationUnit a unit of time
		 * @return {@code this}
		 */
		public Builder convertDurationsTo(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		/**
		 * Only report metrics which match the given filter.
		 *
		 * @param filter a {@link MetricFilter}
		 * @return {@code this}
		 */
		public Builder filter(MetricFilter filter) {
			this.filter = filter;
			return this;
		}

		/**
		 * Only report metrics that have changed.
		 *
		 * @param skipIdleMetrics
		 * @return {@code this}
		 */
		public Builder skipIdleMetrics(boolean skipIdleMetrics) {
			this.skipIdleMetrics = skipIdleMetrics;
			return this;
		}

		/**
		 * Builds a {@link InfluxdbReporter} with the given properties, sending
		 * metrics using the given {@link Influxdb} client.
		 *
		 * @param influxdb a {@link Influxdb} client
		 * @return a {@link InfluxdbReporter}
		 */
		public InfluxdbReporter build(Influxdb influxdb) {
			return new InfluxdbReporter(registry,
					influxdb,
					clock,
					prefix,
					rateUnit,
					durationUnit,
					filter,
					skipIdleMetrics);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(InfluxdbReporter.class);

	private final Influxdb influxdb;
	private final Clock clock;
	private final String prefix;
	private final boolean skipIdleMetrics;
	private final Map<String, Long> previousValues;

	private InfluxdbReporter(MetricRegistry registry,
			Influxdb influxdb,
			Clock clock,
			String prefix,
			TimeUnit rateUnit,
			TimeUnit durationUnit,
			MetricFilter filter,
			boolean skipIdleMetrics) {
		super(registry, "influxdb-reporter", filter, rateUnit, durationUnit);
		this.influxdb = influxdb;
		this.clock = clock;
		this.prefix = (prefix == null) ? "" : (prefix.trim() + ".");
		this.skipIdleMetrics = skipIdleMetrics;
		this.previousValues = new TreeMap<String, Long>();
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
		final SeriesData data = new SeriesData.Builder(influxdb.shouldIncludeTimestamps())
			.columns(COLUMNS_TIMER)
			.addPoint(
				timestamp,
				snapshot.size(),
				convertDuration(snapshot.getMin()),
				convertDuration(snapshot.getMax()),
				convertDuration(snapshot.getMean()),
				convertDuration(snapshot.getStdDev()),
				convertDuration(snapshot.getMedian()),
				convertDuration(snapshot.get75thPercentile()),
				convertDuration(snapshot.get95thPercentile()),
				convertDuration(snapshot.get99thPercentile()),
				convertDuration(snapshot.get999thPercentile()),
				convertRate(timer.getOneMinuteRate()),
				convertRate(timer.getFiveMinuteRate()),
				convertRate(timer.getFifteenMinuteRate()),
				convertRate(timer.getMeanRate()),
				timer.getCount()
			)
			.build();

		influxdb.appendSeries(prefix, name, ".timer", data);
	}

	private void reportHistogram(String name, Histogram histogram, long timestamp) {
		if (canSkipMetric(name, histogram)) {
			return;
		}

		final Snapshot snapshot = histogram.getSnapshot();
		final SeriesData data = new SeriesData.Builder(influxdb.shouldIncludeTimestamps())
			.columns(COLUMNS_HISTOGRAM)
			.addPoint(
				timestamp,
				snapshot.size(),
				snapshot.getMin(),
				snapshot.getMax(),
				snapshot.getMean(),
				snapshot.getStdDev(),
				snapshot.getMedian(),
				snapshot.get75thPercentile(),
				snapshot.get95thPercentile(),
				snapshot.get99thPercentile(),
				snapshot.get999thPercentile(),
				histogram.getCount()
			)
			.build();

		influxdb.appendSeries(prefix, name, ".histogram", data);
	}

	private void reportCounter(String name, Counter counter, long timestamp) {
		final SeriesData data = new SeriesData.Builder(influxdb.shouldIncludeTimestamps())
			.columns(COLUMNS_COUNT)
			.addPoint(
				timestamp,
				counter.getCount()
			)
			.build();

		influxdb.appendSeries(prefix, name, ".count", data);
	}

	private void reportGauge(String name, Gauge<?> gauge, long timestamp) {
		final SeriesData data = new SeriesData.Builder(influxdb.shouldIncludeTimestamps())
			.columns(COLUMNS_GAUGE)
			.addPoint(
				timestamp,
				gauge.getValue()
			)
			.build();

		influxdb.appendSeries(prefix, name, ".value", data);
	}

	private void reportMeter(String name, Metered meter, long timestamp) {
		if (canSkipMetric(name, meter)) {
			return;
		}

		final SeriesData data = new SeriesData.Builder(influxdb.shouldIncludeTimestamps())
			.columns(COLUMNS_METER)
			.addPoint(
				timestamp,
				meter.getCount(),
				convertRate(meter.getOneMinuteRate()),
				convertRate(meter.getFiveMinuteRate()),
				convertRate(meter.getFifteenMinuteRate()),
				convertRate(meter.getMeanRate())
			)
			.build();

		influxdb.appendSeries(prefix, name, ".meter", data);
	}

	// private String format(Object o) {
	// if (o instanceof Float) {
	// return format(((Float) o).doubleValue());
	// } else if (o instanceof Double) {
	// return format(((Double) o).doubleValue());
	// } else if (o instanceof Byte) {
	// return format(((Byte) o).longValue());
	// } else if (o instanceof Short) {
	// return format(((Short) o).longValue());
	// } else if (o instanceof Integer) {
	// return format(((Integer) o).longValue());
	// } else if (o instanceof Long) {
	// return format(((Long) o).longValue());
	// }
	// return null;
	// }
	// private String format(long n) {
	// return Long.toString(n);
	// }
	//
	// private String format(double v) {
	// // the Carbon plaintext format is pretty underspecified, but it seems like
	// it just wants
	// // US-formatted digits
	// return String.format(Locale.US, "%2.2f", v);
	// }

	/**
	 * Returns true if this metric is idle and should be skipped.
	 *
	 * @param name
	 * @param counting
	 * @return true if the metric should be skipped
	 */
	private boolean canSkipMetric(String name, Counting counting) {
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
}
