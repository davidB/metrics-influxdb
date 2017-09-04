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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import metrics_influxdb.api.measurements.MetricMeasurementTransformer;
import metrics_influxdb.measurements.HttpInlinerSender;
import metrics_influxdb.measurements.MeasurementReporter;
import metrics_influxdb.measurements.Sender;
import metrics_influxdb.measurements.UdpInlinerSender;
import metrics_influxdb.measurements.reporter.CounterMeasurementReporter;
import metrics_influxdb.measurements.reporter.GaugeMeasurementReporter;
import metrics_influxdb.measurements.reporter.HistogramMeasurementReporter;
import metrics_influxdb.measurements.reporter.MeterMeasurementReporter;
import metrics_influxdb.measurements.reporter.TimerMeasurementReporter;
import metrics_influxdb.misc.Miscellaneous;
import metrics_influxdb.misc.VisibilityIncreasedForTests;
import metrics_influxdb.v08.Influxdb;
import metrics_influxdb.v08.InfluxdbHttp;
import metrics_influxdb.v08.InfluxdbUdp;
import metrics_influxdb.v08.ReporterV08;

/**
 * A reporter which publishes metric values to a InfluxDB server.
 *
 * @see <a href="http://influxdb.org/">InfluxDB - An open-source distributed
 *      time series database with no external dependencies.</a>
 */
public class InfluxdbReporter  {

	static enum InfluxdbCompatibilityVersions {
		V08, LATEST;
	}

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
		private ScheduledExecutorService executor;
    private CounterMeasurementReporter counterMeasurementReporter;
		private GaugeMeasurementReporter gaugeMeasurementReporter;
		private HistogramMeasurementReporter histogramMeasurementReporter;
		private MeterMeasurementReporter meterMeasurementReporter;
		private TimerMeasurementReporter timerMeasurementReporter;


		@VisibilityIncreasedForTests InfluxdbCompatibilityVersions influxdbVersion;
		@VisibilityIncreasedForTests InfluxdbProtocol protocol;
		@VisibilityIncreasedForTests Influxdb influxdbDelegate;
		@VisibilityIncreasedForTests Map<String, String> tags;
		@VisibilityIncreasedForTests MetricMeasurementTransformer transformer = MetricMeasurementTransformer.NOOP;

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
			this.protocol = new HttpInfluxdbProtocol();
			this.influxdbVersion = InfluxdbCompatibilityVersions.LATEST;
			this.tags = new HashMap<>();
      this.counterMeasurementReporter = new CounterMeasurementReporter();
			this.gaugeMeasurementReporter = new GaugeMeasurementReporter();
			this.histogramMeasurementReporter = new HistogramMeasurementReporter();
			this.meterMeasurementReporter = new MeterMeasurementReporter(TimeUnit.SECONDS);
			this.timerMeasurementReporter = new TimerMeasurementReporter(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false);
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

		public Builder withScheduler(ScheduledExecutorService executor) {
			this.executor = executor;
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
		 * Add a custom measurement reporter for counters.
		 *
		 * @param measurementReporter a {@link CounterMeasurementReporter}
		 * @return {@code this}
		 */
		public Builder counterMeasurementReporter(CounterMeasurementReporter measurementReporter) {
			this.counterMeasurementReporter = measurementReporter;
			return this;
		}

		/**
		 * Add a custom measurement reporter for gauges.
		 *
		 * @param measurementReporter a {@link GaugeMeasurementReporter}
		 * @return {@code this}
		 */
		public Builder gaugeMeasurementReporter(GaugeMeasurementReporter measurementReporter) {
			this.gaugeMeasurementReporter = measurementReporter;
			return this;
		}

		/**
		 * Add a custom measurement reporter for histograms.
		 *
		 * @param measurementReporter a {@link HistogramMeasurementReporter}
		 * @return {@code this}
		 */
		public Builder histogramMeasurementReporter(HistogramMeasurementReporter measurementReporter) {
			this.histogramMeasurementReporter = measurementReporter;
			return this;
		}

		/**
		 * Add a custom measurement reporter for meters.
		 *
		 * @param measurementReporter a {@link MeterMeasurementReporter}
		 * @return {@code this}
		 */
		public Builder meterMeasurementReporter(MeterMeasurementReporter measurementReporter) {
			this.meterMeasurementReporter = measurementReporter;
			return this;
		}

		/**
		 * Add a custom measurement reporter for timers.
		 *
		 * @param measurementReporter a {@link TimerMeasurementReporter}
		 * @return {@code this}
		 */
		public Builder timerMeasurementReporter(TimerMeasurementReporter measurementReporter) {
			this.timerMeasurementReporter = measurementReporter;
			return this;
		}

		/**
		 * Builds a {@link ScheduledReporter} with the given properties, sending
		 * metrics using the given InfluxDB.
		 *
		 * @return a {@link ScheduledReporter}
		 */
		public ScheduledReporter build() {
			ScheduledReporter reporter;

			switch (influxdbVersion) {
			case V08:
				Influxdb influxdb = buildInfluxdb();
				reporter = (executor == null)
						? new ReporterV08(registry, influxdb, clock, prefix, rateUnit, durationUnit, filter, skipIdleMetrics)
						: new ReporterV08(registry, influxdb, clock, prefix, rateUnit, durationUnit, filter, skipIdleMetrics, executor)
						;
				break;
			default:
				if (timerMeasurementReporter == null) {
					timerMeasurementReporter = new TimerMeasurementReporter(rateUnit, durationUnit, false);
				}

				if (meterMeasurementReporter == null) {
					meterMeasurementReporter = new MeterMeasurementReporter(rateUnit);
				}

				if (executor != null) {
					return new MeasurementReporter(buildSender(), registry, filter, rateUnit, durationUnit, clock, tags, transformer, counterMeasurementReporter, gaugeMeasurementReporter, histogramMeasurementReporter, meterMeasurementReporter, timerMeasurementReporter, executor);
				} else {
					return new MeasurementReporter(buildSender(), registry, filter, rateUnit, durationUnit, clock, tags, transformer, counterMeasurementReporter, gaugeMeasurementReporter, histogramMeasurementReporter, meterMeasurementReporter, timerMeasurementReporter);
				}
			}
			return reporter;
		}

		/**
		 * Operates with influxdb version less or equal than 08.
		 * @return the builder itself
		 */
		public Builder v08() {
			this.influxdbVersion  = InfluxdbCompatibilityVersions.V08;
			return this;
		}

		/**
		 * Override the protocol to use.
		 * @param protocol a non null protocol
		 * @return
		 */
		public Builder protocol(InfluxdbProtocol protocol) {
			Objects.requireNonNull(protocol, "given InfluxdbProtocol cannot be null");
			this.protocol = protocol;
			return this;
		}

		/**
		 * Sets the metric2measurement transformer to be used.
		 * @param transformer a non null transformer
		 * @return
		 */
		public Builder transformer(MetricMeasurementTransformer transformer) {
			Objects.requireNonNull(transformer, "given MetricMeasurementTransformer cannot be null");
			this.transformer = transformer;
			return this;
		}

		/**
		 * Registers the given key/value as a default tag for the generated measurements.
		 * @param tagKey the key to register, cannot be null or empty
		 * @param tagValue the value to register against the given key, cannot be null or empty
		 */
		public Builder tag(String tagKey, String tagValue) {
			Miscellaneous.requireNotEmptyParameter(tagKey, "tag");
			Miscellaneous.requireNotEmptyParameter(tagValue, "value");
			tags.put(tagKey, tagValue);
			return this;
		}

		private Influxdb buildInfluxdb() {
			if (protocol instanceof HttpInfluxdbProtocol) {
				try {
					HttpInfluxdbProtocol p = (HttpInfluxdbProtocol) protocol;
					return new InfluxdbHttp(p.scheme, p.host, p.port, p.database, p.user, p.password, durationUnit);
				} catch(RuntimeException exc) {
					throw exc;
				} catch(Exception exc) {
					// wrap exception into RuntimeException
					throw new RuntimeException(exc.getMessage(), exc);
				}
			} else if (protocol instanceof UdpInfluxdbProtocol) {
				UdpInfluxdbProtocol p = (UdpInfluxdbProtocol) protocol;
				return new InfluxdbUdp(p.host, p.port);
			} else {
				throw new IllegalStateException("unsupported protocol: " + protocol);
			}
		}

		private Sender buildSender() {
			if (protocol instanceof HttpInfluxdbProtocol) {
				return new HttpInlinerSender((HttpInfluxdbProtocol) protocol);
				// TODO allow registration of transformers
				// TODO evaluate need of prefix (vs tags)
			} else if (protocol instanceof UdpInfluxdbProtocol) {
				return new UdpInlinerSender((UdpInfluxdbProtocol) protocol);
			} else {
				throw new IllegalStateException("unsupported protocol: " + protocol);
			}

		}
	}
}
