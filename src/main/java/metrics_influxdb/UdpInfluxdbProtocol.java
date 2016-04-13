package metrics_influxdb;

public class UdpInfluxdbProtocol implements InfluxdbProtocol {
	public final String host;
	public final int port;

	public UdpInfluxdbProtocol(String host, int port) {
		this.host = host;
		this.port = port;
	}
}
