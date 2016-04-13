package metrics_influxdb.v08;

public interface Influxdb {
	public void resetRequest();
	public boolean hasSeriesData();
	public long convertTimestamp(long timestamp);
	public void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points);
	public int sendRequest(boolean throwExc, boolean printJson) throws Exception;
}
