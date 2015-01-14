package metrics_influxdb;

import java.util.ArrayList;
import java.util.Arrays;

public class SeriesData {
	protected final ArrayList<String> columns;
	protected final ArrayList<ArrayList<Object>> points;

	private SeriesData(Builder builder) {
		this.columns = builder.columns;
		this.points = builder.points;
	}

	public static class Builder {
		private ArrayList<String> columns;
		private ArrayList<ArrayList<Object>> points;
		private boolean includeTimestamps;
		private int timestampIndex = -1;

		public Builder(boolean includeTimestamps) {
			this.includeTimestamps = includeTimestamps;
			this.points = new ArrayList<>();
		}

		public Builder columns(String[] columns) {
			this.columns = new ArrayList<>();
			this.columns.addAll(Arrays.asList(columns));

			if (!includeTimestamps) {
				timestampIndex = this.columns.indexOf("time");

				if (timestampIndex != -1) {
					this.columns.remove(timestampIndex);
				}
			}

			return this;
		}

		public Builder addPoint(Object... columnValues) {
			ArrayList<Object> point = new ArrayList<>();

			for (int i = 0; i < columnValues.length; ++i) {
				if (!includeTimestamps && i == timestampIndex) {
					continue;
				}

				point.add(columnValues[i]);
			}

			assert point.size() == columns.size();
			points.add(point);
			return this;
		}

		public SeriesData build() {
			return new SeriesData(this);
		}
	}
}
