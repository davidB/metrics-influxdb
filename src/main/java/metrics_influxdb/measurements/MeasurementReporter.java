package metrics_influxdb.measurements;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import metrics_influxdb.SkipIdleReporter;
import metrics_influxdb.api.measurements.MetricMeasurementTransformer;

public class MeasurementReporter extends SkipIdleReporter {
    private final Sender sender;
    private final Clock clock;
    private Map<String, String> baseTags;
    private MetricMeasurementTransformer transformer;

    public MeasurementReporter(Sender sender, MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, boolean skipIdleMetrics, Clock clock, Map<String, String> baseTags, MetricMeasurementTransformer transformer) {
        super(registry, "measurement-reporter", filter, rateUnit, durationUnit, skipIdleMetrics);
        this.baseTags = baseTags;
        this.sender = sender;
        this.clock = clock;
        this.transformer = transformer;
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
            Collection<Measurement> measures = fromGauge(entry.getKey(), entry.getValue(), timestamp);
            sender.send(measures);
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            Collection<Measurement> measures = fromCounter(entry.getKey(), entry.getValue(), timestamp);
            sender.send(measures);
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            Collection<Measurement> measures = fromHistogram(entry.getKey(), entry.getValue(), timestamp);
            sender.send(measures);
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            Collection<Measurement> measures = fromMeter(entry.getKey(), entry.getValue(), timestamp);
            sender.send(measures);
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            Collection<Measurement> measures = fromTimer(entry.getKey(), entry.getValue(), timestamp);
            sender.send(measures);
        }

        sender.flush();
    }

    private Collection<Measurement> fromTimer(String metricName, Timer t, long timestamp) {
        Snapshot snapshot = t.getSnapshot();

        Map<String, String> tags = new HashMap<String, String>(baseTags);
        tags.putAll(transformer.getTagsExtractor().apply(metricName));

        Measure measure = new Measure(transformer.getMeasurementNamer().apply(metricName))
                .timestamp(timestamp)
                .addTag(tags)
                .addValue("count", snapshot.size())
                .addValue("min", convertDuration(snapshot.getMin()))
                .addValue("max", convertDuration(snapshot.getMax()))
                .addValue("mean", convertDuration(snapshot.getMean()))
                .addValue("std-dev", convertDuration(snapshot.getStdDev()))
                .addValue("50-percentile", convertDuration(snapshot.getMedian()))
                .addValue("75-percentile", convertDuration(snapshot.get75thPercentile()))
                .addValue("95-percentile", convertDuration(snapshot.get95thPercentile()))
                .addValue("99-percentile", convertDuration(snapshot.get99thPercentile()))
                .addValue("999-percentile", convertDuration(snapshot.get999thPercentile()))
                .addValue("one-minute", convertRate(t.getOneMinuteRate()))
                .addValue("five-minute", convertRate(t.getFiveMinuteRate()))
                .addValue("fifteen-minute", convertRate(t.getFifteenMinuteRate()))
                .addValue("mean-minute", convertRate(t.getMeanRate()))
                .addValue("run-count", t.getCount());
        
        return Collections.singleton(measure);
    }

    private Collection<Measurement> fromMeter(String metricName, Meter mt, long timestamp) {
        Map<String, String> tags = new HashMap<String, String>(baseTags);
        tags.putAll(transformer.getTagsExtractor().apply(metricName));

        Measure measure = new Measure(transformer.getMeasurementNamer().apply(metricName))
                .timestamp(timestamp)
                .addTag(tags)
                .addValue("count", mt.getCount())
                .addValue("one-minute", convertRate(mt.getOneMinuteRate()))
                .addValue("five-minute", convertRate(mt.getFiveMinuteRate()))
                .addValue("fifteen-minute", convertRate(mt.getFifteenMinuteRate()))
                .addValue("mean-minute", convertRate(mt.getMeanRate()));
        return Collections.singleton(measure);
    }

    private Collection<Measurement> fromHistogram(String metricName, Histogram h, long timestamp) {
        Snapshot snapshot = h.getSnapshot();
        
        Map<String, String> tags = new HashMap<String, String>(baseTags);
        tags.putAll(transformer.getTagsExtractor().apply(metricName));

        Measure measure = new Measure(transformer.getMeasurementNamer().apply(metricName))
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
                .addValue("run-count", h.getCount());
        return Collections.singleton(measure);
    }

    private Collection<Measurement> fromCounter(String metricName, Counter c, long timestamp) {
        Map<String, String> tags = new HashMap<String, String>(baseTags);
        tags.putAll(transformer.getTagsExtractor().apply(metricName));

        Measure measure = new Measure(transformer.getMeasurementNamer().apply(metricName))
                .timestamp(timestamp)
                .addTag(tags)
                .addValue("count", c.getCount());
            
        return Collections.singleton(measure);
    }

    @SuppressWarnings("rawtypes")
    private Collection<Measurement> fromGauge(String metricName, Gauge g, long timestamp) {
        Map<String, String> tags = new HashMap<String, String>(baseTags);
        tags.putAll(transformer.getTagsExtractor().apply(metricName));

        Measure measure = new Measure(transformer.getMeasurementNamer().apply(metricName))
                .timestamp(timestamp)
                .addTag(tags);
        Object o = g.getValue();
        if (o instanceof Long || o instanceof Integer) {
            long value = ((Number)o).longValue();
            measure.addValue("value", value);
        } else if (o instanceof Double || o instanceof Float) {
            double value = ((Number)o).doubleValue();
            measure.addValue("value", value);
        } else {
            String value = ""+o;
            measure.addValue("value", value);
        }
        
        return Collections.singleton(measure);
    }
}
