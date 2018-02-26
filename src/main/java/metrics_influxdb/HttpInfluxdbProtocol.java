package metrics_influxdb;

public class HttpInfluxdbProtocol implements InfluxdbProtocol {
	public final static String DEFAULT_HOST = "127.0.0.1";
	public final static int DEFAULT_PORT = 8086;
	public final static String DEFAULT_DATABASE = "metrics";
	public final static long DEFAULT_CONNECT_TIMEOUT_SECONDS = 2;
	public final static long DEFAULT_READ_TIMEOUT_SECONDS = 2;
    
	public final String scheme;
	public final String user;
	public final String password;
	public final String host; 
	public final int port;
	public final boolean secured;
	public final String database;
	public final long connectTimeout;
	public final long readTimeout;

	public HttpInfluxdbProtocol(String scheme, String host, int port, String user, String password, String db, long connectTimeout, long readTimeout) {
		super();
		this.scheme = scheme;
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.database = db;
		this.secured = (user != null) && (password != null);
		this.connectTimeout = connectTimeout;
		this.readTimeout = readTimeout;
	}
	
	public HttpInfluxdbProtocol(String scheme, String host, int port, String user, String password, String db) {
	    this(scheme, host, port, user, password, db, DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS);
	}
	
	public HttpInfluxdbProtocol(String host, int port, String user, String password, String db) {
		this("http", host, port, user, password, db);
	}

	public HttpInfluxdbProtocol(String host) {
		this(host, DEFAULT_PORT);
	}

	public HttpInfluxdbProtocol(String host, int port) {
		this(host, port, null, null);
	}

	public HttpInfluxdbProtocol(String host, int port, String database) {
		this(host, port, null, null, database);
	}

	public HttpInfluxdbProtocol() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}

	public HttpInfluxdbProtocol(String host, int port, String user, String password) {
		this(host, port, user, password, DEFAULT_DATABASE);
	}
}
