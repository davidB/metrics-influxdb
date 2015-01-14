package metrics_influxdb;

import java.util.ArrayList;
import java.util.Collection;

class JsonBuilderDefault implements JsonBuilder {
	private final StringBuilder json = new StringBuilder();
	private boolean hasSeriesData;

	@Override
	public boolean hasSeriesData() {
		return hasSeriesData;
	}

	@Override
	public void reset() {
		json.setLength(0);
		json.append('[');
		hasSeriesData = false;
	}

	@Override
	public String toJsonString() {
		json.append(']');
		String str = json.toString();
		json.setLength(json.length() - 1);
		return str;
	}

	@Override
	public void appendSeries(String namePrefix,String  name,String nameSuffix, SeriesData data) {
		hasSeriesData = true;

		if (json.length() > 1) {
			json.append(',');
		}

		json.append("{\"name\":\"").append(namePrefix).append(name).append(nameSuffix).append("\",\"columns\":[");

		for (int i = 0; i < data.columns.size(); i++) {
			if (i > 0) {
				json.append(',');
			}

			json.append('"').append(data.columns.get(i)).append('"');
		}

		json.append("],\"points\":[");

		for (int i = 0; i < data.points.size(); i++) {
			if (i > 0) {
				json.append(',');
			}

			ArrayList<Object> row = data.points.get(i);
			json.append('[');

			for (int j = 0; j < row.size(); j++) {
				if (j > 0)
					json.append(',');
				Object value = row.get(j);
				if (value instanceof String) {
					json.append('"').append(value).append('"');
				} else if((value instanceof Collection) && ((Collection<?>)value).size()<1) {
					json.append("null");
				} else {
					json.append(value);
				}
			}

			json.append(']');
		}

		json.append("]}");
	}

	/* (non-Javadoc)
	 * @see metrics_influxdb.JsonBuilder#toString()
	 */
	@Override
	public String toString() {
		return json.toString();
	}

}
