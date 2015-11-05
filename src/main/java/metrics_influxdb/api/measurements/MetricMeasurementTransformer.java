package metrics_influxdb.api.measurements;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public class MetricMeasurementTransformer {
    private final Function<String, Map<String, String>> tagsExtractor;
    private final Function<String, String> measurementNamer;

    public MetricMeasurementTransformer(Function<String, Map<String, String>> tagsExtractor, Function<String, String> measurementNamer) {
        this.tagsExtractor = tagsExtractor;
        this.measurementNamer = measurementNamer;
    }

    public Function<String, Map<String, String>> getTagsExtractor() {
        return tagsExtractor;
    }

    public Function<String, String> getMeasurementNamer() {
        return measurementNamer;
    }
    
    public static final MetricMeasurementTransformer NOOP = new MetricMeasurementTransformer(m -> Collections.emptyMap(), m -> m);
}
