package metrics_influxdb.api.measurements;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@link KeyValueMetricMeasurementTransformer} uses metric name as if if contained a pattern of key/value in its name.
 * The pattern is as follow:
 * <pre>key1.val1.key2.val2. ... .key2n+1.value2n+1.metric-name</pre>
 * Finally this transformer will generate a measurement named `metric-name` with tags [[key1=val1],[key2=val2], ..., [key2n+1=val2n+1]].
 * 
 * If when splitting the initial metric name using the '.' character, the number of strings is not even, then the last 2 strings will be used to generate
 * the measurement-name.
 * <br>
 *  Examples:
 *  <ul>
 *  <li>`server.actarus.cpu_load` will be transformed to a measurement called `cpu_load` with tags [[server=actarus]]</li>
 *  <li>`server.actarus.cores.cpu_load` will be transformed to a measurement called `cores.cpu_load` with tags [[server=actarus]]</li>
 *  </ul>
 */
public class KeyValueMetricMeasurementTransformer implements MetricMeasurementTransformer {
	private final static String SEPARATOR = "\\.";

	public KeyValueMetricMeasurementTransformer() {
	}

	@Override
	public Map<String, String> tags(String metricName) {
		Map<String, String> generatedTags = new HashMap<>();
		String[] splitted = metricName.split(SEPARATOR);

		int nbPairs = isEven(splitted.length)?(splitted.length-1)/2:(splitted.length/2)-1;

		for (int i = 0; i < nbPairs; i++) {
			generatedTags.put(splitted[2*i], splitted[2*i+1]);
		}

		return generatedTags;
	}

	public boolean isEven(int number) {
		return (number % 2)==1;
	}

	@Override
	public String measurementName(String metricName) {
		String[] splitted = metricName.split(SEPARATOR);

		if (isEven(splitted.length)) {
			return splitted[splitted.length - 1];
		} else {
			return splitted[splitted.length - 2] + "." + splitted[splitted.length - 1];
		}
	}
}
