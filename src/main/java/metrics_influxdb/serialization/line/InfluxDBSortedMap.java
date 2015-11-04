package metrics_influxdb.serialization.line;

import java.util.TreeMap;

/**
 * Not sure if something needs to be done to match golang byte comparison as described in influxdb <a href="https://influxdb.com/docs/v0.9/write_protocols/line.html">documentation</a>
 * Let's use a simple TreeMap with no comparator so that lexical order will be used.
 */
public class InfluxDBSortedMap extends TreeMap<String, String> {
	private static final long serialVersionUID = -7218529992523196655L;

	public InfluxDBSortedMap() {
	}
}
