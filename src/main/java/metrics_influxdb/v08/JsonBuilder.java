package metrics_influxdb.v08;

interface JsonBuilder {

	/**
	 * Returns true if this builder has series data to send.
	 */
	public abstract boolean hasSeriesData();

	/**
	 * Forget previous appendSeries.
	 */
	public abstract void reset();

	/**
	 * generate the json as String.
	 */
	public abstract String toJsonString();

	/**
	 * Append series of data into the next Request to send.
	 *
	 * @param namePrefix
	 * @param name
	 * @param nameSuffix
	 * @param columns
	 * @param points
	 */
	public abstract void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points);
}