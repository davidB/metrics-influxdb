package metrics_influxdb.serialization.line;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import metrics_influxdb.measurements.Measure;
import metrics_influxdb.misc.Miscellaneous;

public class Inliner {
		private static char[] ESCAPE_CHARS = {' ', ',', '='};

		private TimeUnit precision;

		public Inliner(TimeUnit precision) {
			this.precision = precision;
		}

		public String inline(Measure m) {
		String key = buildMeasureKey(m.getName(), m.getTags());
		String values = buildMeasureFields(m.getValues());
		String timestamp = "" + precision.convert(m.getTimestamp(), TimeUnit.MILLISECONDS);

		return key + " " + values +  " " + timestamp;
	}

	public String inline(Iterable<Measure> measures) {
		StringBuilder sb = new StringBuilder();
		String join = "";
		String cr = "\n";
		for (Measure m : measures) {
			sb.append(join).append(inline(m));
			join = cr;
		}
		return sb.toString();
	}

	private String buildMeasureFields(Map<String, String> values) {
		Map<String, String> sortedValues = new InfluxDBSortedMap();
		sortedValues.putAll(values);

		StringBuilder fields = new StringBuilder();
		String join = "";

		for (Map.Entry<String, String> v: sortedValues.entrySet()) {
			fields.append(join).append(Miscellaneous.escape(v.getKey(), ESCAPE_CHARS)).append("=").append(v.getValue());		// values are already escaped
			join = ",";
		}
		return fields.toString();
	}

	private String buildMeasureKey(String name, Map<String, String> tags) {
		StringBuilder key = new StringBuilder(Miscellaneous.escape(name, ESCAPE_CHARS));
		Map<String, String> sortedTags = new InfluxDBSortedMap();
		sortedTags.putAll(tags);

		for (Map.Entry<String, String> e: sortedTags.entrySet()) {
			key.append(',').append(Miscellaneous.escape(e.getKey(), ESCAPE_CHARS)).append("=").append(Miscellaneous.escape(e.getValue(), ESCAPE_CHARS));
		}

		return key.toString();
	}

}
