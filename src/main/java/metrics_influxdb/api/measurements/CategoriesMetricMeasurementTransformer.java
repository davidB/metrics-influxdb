package metrics_influxdb.api.measurements;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This transformer uses a provided list of categories in order to extract tag values from the given metrics name.
 * Each metric name is splitted, and each splitted string falls into a given category bucket.
 * <br>
 * Example using the categories ["server", "type"] a metric called `actarus.production.cpu_load` will be turned into
 * a measurement:
 * <pre>
 *    name: cpu_load
 *    tags: [[server=actarus], [type=production]]
 * </pre>
 */
public class CategoriesMetricMeasurementTransformer implements MetricMeasurementTransformer {
	private final static String SEPARATOR = "\\.";
	private final String[] categories;

	public  CategoriesMetricMeasurementTransformer(String ... categories) {
		this.categories = categories;
	}

	@Override
	public Map<String, String> tags(String metricName) {
		HashMap<String, String> tags = new HashMap<>();

		String[] splitted = metricName.split(SEPARATOR);

		int nbSplittedToUse = Math.min(splitted.length-1, categories.length);
		for (int i = 0; i < nbSplittedToUse; i++) {
			tags.put(categories[i], splitted[i]);
		}

		return tags;
	}

	@Override
	public String measurementName(String metricName) {
		String[] splitted = metricName.split(SEPARATOR);

		String[] toUseInMeasurement;
		if (categories.length < splitted.length) {
			toUseInMeasurement = Arrays.copyOfRange(splitted, categories.length, splitted.length);
		} else {
			// too many categories compared to splitted values
			// we consider only the last splitted value
			toUseInMeasurement = new String[] {splitted[splitted.length - 1]};
		}

		return String.join(".", toUseInMeasurement);
	}
}
