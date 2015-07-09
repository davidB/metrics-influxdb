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

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
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
public class InfluxdbReporter extends ScheduledReporter {
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
		private String tag;

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
			this.tag = "";
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

		public Builder withTag(String tag){
			this.tag = tag;
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
					skipIdleMetrics,
					tag);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(InfluxdbReporter.class);

	private final Influxdb influxdb;
	private final Clock clock;
	private final String prefix;
	private final boolean skipIdleMetrics;
	private final Map<String, Long> previousValues;
	private final InfluxdbReporterHelper reporterHelper;

	private InfluxdbReporter(MetricRegistry registry,
							 Influxdb influxdb,
							 Clock clock,
							 String prefix,
							 TimeUnit rateUnit,
							 TimeUnit durationUnit,
							 MetricFilter filter,
							 boolean skipIdleMetrics,
							 String tag) {
		super(registry, "influxdb-reporter", filter, rateUnit, durationUnit);
		this.influxdb = influxdb;
		this.clock = clock;
		this.prefix = (prefix == null) ? "" : (prefix.trim() + ".");
		this.skipIdleMetrics = skipIdleMetrics;
		this.previousValues = new TreeMap<String, Long>();
		this.reporterHelper = new InfluxdbReporterHelper(tag);
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
		Object[] p = reporterHelper.getPointsTimer()[0];
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
		assert (p.length == reporterHelper.getColumnsTimer().length);
		influxdb.appendSeries(prefix, name, ".timer", reporterHelper.getColumnsTimer(), reporterHelper.getPointsTimer());
	}

	private void reportHistogram(String name, Histogram histogram, long timestamp) {
		if (canSkipMetric(name, histogram)) {
			return;
		}
		final Snapshot snapshot = histogram.getSnapshot();
		Object[] p = reporterHelper.getPointsHistogram()[0];
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
		assert (p.length == reporterHelper.getColumnsHistogram().length);
		influxdb.appendSeries(prefix, name, ".histogram", reporterHelper.getColumnsHistogram(), reporterHelper.getPointsHistogram());
	}

	private void reportCounter(String name, Counter counter, long timestamp) {
		Object[] p = reporterHelper.getPointsCounter()[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = counter.getCount();
		assert (p.length == reporterHelper.getColumnsCount().length);
		influxdb.appendSeries(prefix, name, ".count", reporterHelper.getColumnsCount(), reporterHelper.getPointsCounter());
	}

	private void reportGauge(String name, Gauge<?> gauge, long timestamp) {
		Object[] p = reporterHelper.getPointsGauge()[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = gauge.getValue();
		assert (p.length == reporterHelper.getColumnsGauge().length);
		influxdb.appendSeries(prefix, name, ".value", reporterHelper.getColumnsGauge(), reporterHelper.getPointsGauge());
	}

	private void reportMeter(String name, Metered meter, long timestamp) {
		if (canSkipMetric(name, meter)) {
			return;
		}
		Object[] p = reporterHelper.getPointsMeter()[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = meter.getCount();
		p[2] = convertRate(meter.getOneMinuteRate());
		p[3] = convertRate(meter.getFiveMinuteRate());
		p[4] = convertRate(meter.getFifteenMinuteRate());
		p[5] = convertRate(meter.getMeanRate());
		assert (p.length == reporterHelper.getColumnsMeter().length);
		influxdb.appendSeries(prefix, name, ".meter", reporterHelper.getColumnsMeter(), reporterHelper.getPointsMeter());
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
