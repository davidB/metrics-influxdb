package metrics_influxdb.api.measurements;

import com.codahale.metrics.Metric;
import metrics_influxdb.measurements.Measure;

/**
 * Interface to adapt measurements reported before they are sent out.
 */
public interface MetricsAdapter {

	/**
	 * @param name    name of the metric
	 * @param metric  the metric to adapt
	 * @param measure measure object (warning, this can and typically will be changed by implementations)
	 * @return returns adapted measure object (can be the same one that was passed in)
	 */
	Measure adapt(String name, Metric metric, Measure measure);

	MetricsAdapter NOOP = new MetricsAdapter() {
		@Override
		public Measure adapt(String name, Metric metric, Measure measure) {
			return measure;
		}
	};

}
