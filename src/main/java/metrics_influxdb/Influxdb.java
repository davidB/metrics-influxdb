package metrics_influxdb;

interface Influxdb {
	void resetRequest();
	boolean hasSeriesData();
	long convertTimestamp(long timestamp);
	void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points);
	int sendRequest(boolean throwExc, boolean printJson) throws Exception;
}
