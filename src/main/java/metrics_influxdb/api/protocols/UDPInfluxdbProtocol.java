package metrics_influxdb.api.protocols;

public class UDPInfluxdbProtocol implements InfluxdbProtocol {
	private final String host;
	private final int port;
	
	public UDPInfluxdbProtocol(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
}
