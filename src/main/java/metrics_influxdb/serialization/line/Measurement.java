package metrics_influxdb.serialization.line;

import java.util.Map;

public interface Measurement {
	public String getName();
	public Map<String, String> getTags();
	public Map<String, String> getValues();
	public long getTimestamp();
}
