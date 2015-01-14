package metrics_influxdb;

interface Influxdb {
	public void resetRequest();
	public boolean hasSeriesData();
	public boolean shouldIncludeTimestamps();
	public void appendSeries(String namePrefix, String name, String nameSuffix, SeriesData data);
	public int sendRequest(boolean throwExc, boolean printJson) throws Exception;
}
