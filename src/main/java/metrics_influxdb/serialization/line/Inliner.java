package metrics_influxdb.serialization.line;

import java.util.Map;

import metrics_influxdb.measurements.Measurement;
import metrics_influxdb.misc.Miscellaneous;

public class Inliner {
	public String inline(Measurement m) {
		String key = buildMeasureKey(m.getName(), m.getTags());
		String values = buildMeasureFields(m.getValues());
		String timestamp = "" + m.getTimestamp();
		
		return key + " " + values +  " " + timestamp;
	}
	
	public String inline(Iterable<Measurement> measurements) {
		StringBuilder sb = new StringBuilder();
		String join = "";
		String cr = "\n";
		for (Measurement m : measurements) {
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
			fields.append(join).append(Miscellaneous.escape(v.getKey(), ' ', ',')).append("=").append(v.getValue());		// values are already escaped
			join = ",";
		}
		return fields.toString();
	}

	private String buildMeasureKey(String name, Map<String, String> tags) {
		StringBuilder key = new StringBuilder(Miscellaneous.escape(name, ' ', ','));
		Map<String, String> sortedTags = new InfluxDBSortedMap();
		sortedTags.putAll(tags);
		
		for (Map.Entry<String, String> e: sortedTags.entrySet()) {
			key.append(',').append(Miscellaneous.escape(e.getKey(), ' ', ',')).append("=").append(Miscellaneous.escape(e.getValue(), ' ', ','));
		}
		
		return key.toString();
	}
	
}
