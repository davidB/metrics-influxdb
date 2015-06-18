package metrics_influxdb;

import java.util.Collection;
import org.joda.time.DateTime;

class JsonBuilderV9 implements JsonBuilder {
	private final StringBuilder pointsJson = new StringBuilder();

	private boolean hasSeriesData;

	private String metadata;


	public JsonBuilderV9(String database, String precision) {
		metadata = "{\"database\":\"" + database + "\",\"precision\":\"" + precision + "\",";
	}

	@Override
	public boolean hasSeriesData() {
		return hasSeriesData;
	}

	@Override
	public void reset() {
		pointsJson.setLength(0);
		pointsJson.append("\"points\":[");
		hasSeriesData = false;
	}

	@Override
	public String toJsonString() {
		pointsJson.append(']');
		String str = metadata + pointsJson.toString() + "}";
		pointsJson.setLength(pointsJson.length() - 1);
		return str;
	}

	@Override
	public void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points) {
		if (hasSeriesData)
			pointsJson.append(',');
		hasSeriesData = true;
		final DateTime timestamp = new DateTime();
		pointsJson.append("{\"measurement\":\"").append(namePrefix).append(name).append(nameSuffix).append("\",\"fields\":{");
		Object[] row = points[0];
		// Ignore the timestamp, instead set it in the format influxDb expects.
		for (int j = 1; j < row.length; j++) {
			if (j > 1) {
				pointsJson.append(',');
			}
			Object value = row[j];
			pointsJson.append("\"" + columns[j] + "\":");
			if (value instanceof String) {
				pointsJson.append('"').append(value).append('"');
			} else if ((value instanceof Collection) && ((Collection<?>) value).size() < 1) {
				pointsJson.append("null");
			} else if (value instanceof Double && Double.isInfinite((double) value)) {
				pointsJson.append("null");
			} else if (value instanceof Float && Float.isInfinite((float) value)) {
				pointsJson.append("null");
			} else {
				pointsJson.append(value);
			}
		}
		pointsJson.append("},");

		pointsJson.append("\"timestamp\": \"" + timestamp + "\" }");
	}

	@Override
	public String toString() {
		return pointsJson.toString();
	}

}
