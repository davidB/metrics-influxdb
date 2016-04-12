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
import metrics_influxdb.api.protocols.HttpInfluxdbProtocol;
import metrics_influxdb.api.protocols.InfluxdbProtocol;
import metrics_influxdb.api.protocols.UDPInfluxdbProtocol;
import metrics_influxdb.measurements.HttpInlinerSender;
import metrics_influxdb.measurements.MeasurementReporter;
import metrics_influxdb.measurements.Sender;
import metrics_influxdb.measurements.UDPInlinerSender;
import metrics_influxdb.misc.Miscellaneous;
import metrics_influxdb.misc.VisibilityIncreasedForTests;

/**
 * A reporter which publishes metric values to a InfluxDB server.
 *
 * @see <a href="http://influxdb.org/">InfluxDB - An open-source distributed
 *      time series database with no external dependencies.</a>
 */
public class InfluxdbReporter  {

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

		@VisibilityIncreasedForTests InfluxDBCompatibilityVersions influxdbVersion;
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
			this.influxdbVersion = InfluxDBCompatibilityVersions.LATEST;
			this.tags = new HashMap<>();
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
		 * Builds a {@link InfluxdbReporter} with the given properties, sending
		 * metrics using the given {@link Influxdb} client.
		 *
		 * @param influxdb a {@link Influxdb} client
		 * @return a {@link InfluxdbReporter}
		 */
		public ScheduledReporter build(Influxdb influxdb) {
			return executor == null
					? new ReporterV08(registry, influxdb, clock, prefix, rateUnit, durationUnit, filter, skipIdleMetrics)
					: new ReporterV08(registry, influxdb, clock, prefix, rateUnit, durationUnit, filter, skipIdleMetrics, executor)
					;
		}

		public ScheduledReporter build() {
			ScheduledReporter reporter;

			switch (influxdbVersion) {
			case V08:
				reporter = build(influxdbDelegate);
				break;
			default:
				Sender s = null;
				if (protocol instanceof HttpInfluxdbProtocol) {
					s = new HttpInlinerSender((HttpInfluxdbProtocol) protocol);
					// TODO allow registration of transformers
					// TODO evaluate need of prefix (vs tags)
				} else if (protocol instanceof UDPInfluxdbProtocol) {
					s = new UDPInlinerSender((UDPInfluxdbProtocol) protocol);
				} else {
					throw new IllegalStateException("unsupported protocol: " + protocol);
				}
				reporter = executor == null
						? new MeasurementReporter(s, registry, filter, rateUnit, durationUnit, clock, tags, transformer)
						: new MeasurementReporter(s, registry, filter, rateUnit, durationUnit, clock, tags, transformer, executor)
						;
			}
			return reporter;
		}

		/**
		 * Operates with influxdb version less or equal than 08. 
		 * @param delegate the influxdb delegate to use, cannot be null
		 * @return the builder itself
		 */
		public Builder v08(Influxdb delegate) {
			Objects.requireNonNull(delegate, "given Influxdb cannot be null");
			this.influxdbVersion  = InfluxDBCompatibilityVersions.V08;
			this.influxdbDelegate = delegate;
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
	}
}
