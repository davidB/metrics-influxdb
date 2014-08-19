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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
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
  private static String[] COLUMNS_TIMER = {
      "time", "count"
      , "min", "max", "mean", "std-dev"
      , "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
      , "one-minute", "five-minute", "fifteen-minute", "mean-rate"
  };
  private static String[] COLUMNS_HISTOGRAM = {
      "time", "count"
      , "min", "max", "mean", "std-dev"
      , "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
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
     * @param clock
     *          a {@link Clock} instance
     * @return {@code this}
     */
    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Prefix all metric names with the given string.
     * 
     * @param prefix
     *          the prefix for all metric names
     * @return {@code this}
     */
    public Builder prefixedWith(String prefix) {
      this.prefix = prefix;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     * 
     * @param rateUnit
     *          a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     * 
     * @param durationUnit
     *          a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     * 
     * @param filter
     *          a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Builds a {@link InfluxdbReporter} with the given properties, sending
     * metrics using the given {@link Influxdb} client.
     * 
     * @param influxdb
     *          a {@link Influxdb} client
     * @return a {@link InfluxdbReporter}
     */
    public InfluxdbReporter build(Influxdb influxdb) {
      return new InfluxdbReporter(registry,
          influxdb,
          clock,
          prefix,
          rateUnit,
          durationUnit,
          filter);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxdbReporter.class);

  private final Influxdb influxdb;
  private final Clock clock;
  private final String prefix;
  private final InfluxdbJsonBuilder jsonBuilder = new InfluxdbJsonBuilder();

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
      0.0d
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
      0.0d
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

  private InfluxdbReporter(MetricRegistry registry,
      Influxdb influxdb,
      Clock clock,
      String prefix,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      MetricFilter filter) {
    super(registry, "influxdb-reporter", filter, rateUnit, durationUnit);
    this.influxdb = influxdb;
    this.clock = clock;
    this.prefix = (prefix == null) ? "" : (prefix.trim() + ".");
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
      jsonBuilder.resetJson();
      
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
      jsonBuilder.endJson();
      
      influxdb.sendRequest(jsonBuilder.toString(), true, false);
    } catch (Exception e) {
      LOGGER.warn("Unable to report to InfluxDB. Discarding data.", e);
    }
  }

  private void reportTimer(String name, Timer timer, long timestamp) {
    final Snapshot snapshot = timer.getSnapshot();
    Object[] p = pointsTimer[0];
    p[0] = timestamp;
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
    assert (p.length == COLUMNS_TIMER.length);
    jsonBuilder.appendSeries(prefix, name, ".timer", COLUMNS_TIMER, pointsTimer);
  }

  private void reportHistogram(String name, Histogram histogram, long timestamp) {
    final Snapshot snapshot = histogram.getSnapshot();
    Object[] p = pointsHistogram[0];
    p[0] = timestamp;
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
    assert (p.length == COLUMNS_HISTOGRAM.length);
    jsonBuilder.appendSeries(prefix, name, ".histogram", COLUMNS_HISTOGRAM, pointsHistogram);
  }

  private void reportCounter(String name, Counter counter, long timestamp) {
    Object[] p = pointsCounter[0];
    p[0] = timestamp;
    p[1] = counter.getCount();
    assert (p.length == COLUMNS_COUNT.length);
    jsonBuilder.appendSeries(prefix, name, ".count", COLUMNS_COUNT, pointsCounter);
  }

  private void reportGauge(String name, Gauge<?> gauge, long timestamp) {
    Object[] p = pointsGauge[0];
    p[0] = timestamp;
    p[1] = gauge.getValue();
    assert (p.length == COLUMNS_GAUGE.length);
    jsonBuilder.appendSeries(prefix, name, ".value", COLUMNS_GAUGE, pointsGauge);
  }

  private void reportMeter(String name, Metered meter, long timestamp) {
    Object[] p = pointsMeter[0];
    p[0] = timestamp;
    p[1] = meter.getCount();
    p[2] = convertRate(meter.getOneMinuteRate());
    p[3] = convertRate(meter.getFiveMinuteRate());
    p[4] = convertRate(meter.getFifteenMinuteRate());
    p[5] = convertRate(meter.getMeanRate());
    assert (p.length == COLUMNS_METER.length);
    jsonBuilder.appendSeries(prefix, name, ".meter", COLUMNS_METER, pointsMeter);
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
}
