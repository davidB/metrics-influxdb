package metrics_influxdb.api.protocols;

public class HttpInfluxdbProtocol implements InfluxdbProtocol {
    public final static String DEFAULT_HOST = "127.0.0.1";
    public final static int DEFAULT_PORT = 8086;
    public final static String DEFAULT_DATABASE = "metrics";
    private final String user;
    private final String password;
    private final String host; 
    private final int port;
    private final boolean secured;
    private final String database;
    
    public HttpInfluxdbProtocol(String host, int port, String user, String password, String db) {
        super();
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = db;
        this.secured = (user != null) && (password != null);
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
    
    public String getUser() {
        return user;
    }
    public String getPassword() {
        return password;
    }
    public String getHost() {
        return host;
    }
    public int getPort() {
        return port;
    }
    public boolean isSecured() {
        return secured;
    }

    public String getDatabase() {
        return database;
    }
}
