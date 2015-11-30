package metrics_influxdb.api.measurements;

import java.util.Collections;
import java.util.Map;

public interface MetricMeasurementTransformer {
    public Map<String, String> tags(String metricName);

    public String measurementName(String metricName);
    
    public static final MetricMeasurementTransformer NOOP = new MetricMeasurementTransformer() {
        @Override
        public Map<String, String> tags(String metricName) {
            return Collections.emptyMap();
        }

        @Override
        public String measurementName(String metricName) {
            return metricName;
        }
    };
}
