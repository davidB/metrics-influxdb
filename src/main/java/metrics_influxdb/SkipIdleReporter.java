package metrics_influxdb;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

public abstract class SkipIdleReporter extends ScheduledReporter {
    private final static Logger LOGGER = LoggerFactory.getLogger(SkipIdleReporter.class);
    
    protected final boolean skipIdleMetrics;
    protected final Map<String, Long> previousValues;

    public SkipIdleReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, boolean skipIdleMetrics) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.skipIdleMetrics = skipIdleMetrics;
        this.previousValues = new TreeMap<String, Long>();
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

}