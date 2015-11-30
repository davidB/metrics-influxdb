package metrics_influxdb.api.protocols;

public class InfluxdbProtocols {
	public static InfluxdbProtocol udp(String host, int port) {
		return new UDPInfluxdbProtocol(host, port);
	}
	
    public static InfluxdbProtocol http() {
        return new HttpInfluxdbProtocol();
    }
    
    public static InfluxdbProtocol http(String host) {
        return new HttpInfluxdbProtocol(host);
    }
    
    public static InfluxdbProtocol http(String host, int port) {
        return new HttpInfluxdbProtocol(host, port);
    }
    
    public static InfluxdbProtocol http(String host, int port, String database) {
        return new HttpInfluxdbProtocol(host, port, database);
    }
    
    public static InfluxdbProtocol http(String host, int port, String user, String password) {
        return new HttpInfluxdbProtocol(host, port, user, password);
    }
    
    public static InfluxdbProtocol http(String host, int port, String user, String password, String database) {
        return new HttpInfluxdbProtocol(host, port, user, password, database);
    }
}
