package metrics_influxdb;

interface JsonBuilder {

	/**
	 * Returns true if this builder has series data to send.
	 */
	boolean hasSeriesData();

	/**
	 * Forget previous appendSeries.
	 */
	void reset();

	/**
	 * generate the json as String.
	 */
	String toJsonString();

	/**
	 * Append series of data into the next Request to send.
	 *
	 * @param namePrefix
	 * @param name
	 * @param nameSuffix
	 * @param columns
	 * @param points
	 */
	void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points);
}