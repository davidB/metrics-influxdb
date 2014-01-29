package com.novaquark.metrics.influxdb;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * A client to send data to a InfluxDB server via HTTP protocol.
 * 
 * The usage :
 * <pre>
 *   Influxdb influxdb = new Influxdb(...);

 *   influxdb.appendSeries(...);
 *   ...
 *   influxdb.appendSeries(...);
 *   influxdb.sendRequest();
 *   
 *   influxdb.appendSeries(...);
 *   ...
 *   influxdb.appendSeries(...);
 *   influxdb.sendRequest();
 *   
 * </pre>
 */
public class Influxdb {
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    public static char toTimePrecision(TimeUnit t) {
    	switch(t){
	    	case SECONDS: return 's';
	    	case MILLISECONDS: return 'm';
	    	case MICROSECONDS: return 'u';
	    	default: throw new IllegalArgumentException("time precision should be SECONDS or MILLISECONDS or MICROSECONDS");
    	}
    }
 
    public final URL url;
	private final StringBuilder json = new StringBuilder();

    public Influxdb(String host, int port, String database, String username, String password, TimeUnit timePrecision) throws Exception {
		this(new URL("http", host, port, "/db/" + database + "/series?u=" + URLEncoder.encode(username, UTF_8.name()) + "&p=" + password + "&time_precision=" +toTimePrecision(timePrecision)));
    }

    public Influxdb(URL url) throws Exception {
		this.url = url;
		resetRequest();
    }

    /**
     * Forgot previously appendSeries.
     */
    public void resetRequest(){
    	json.setLength(0);
    	json.append('[');
    }

    /**
     * Append series of data into the next Request to send.
     * @param namePrefix
     * @param name
     * @param nameSuffix
     * @param columns
     * @param points
     */
    public void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points) {
    	if (json.length() > 1) json.append(',');
    	json.append("{\"name\":\"").append(namePrefix).append(name).append(nameSuffix).append("\",\"columns\":[");
    	for(int i=0; i<columns.length; i++) {
    		if (i > 0) json.append(',');
    		json.append('"').append(columns[i]).append('"');
    	}
    	json.append("],\"points\":[");
    	for(int i=0; i<points.length; i++) {
    		if (i > 0) json.append(',');
    		Object[] row = points[i];
    		json.append('[');
    		for(int j=0; j<row.length; j++) {
        		if (j > 0) json.append(',');
        		Object value = row[j];
        		if (value instanceof String) {
        			json.append('"').append(value).append('"');
        		} else {
        			json.append(value);
        		}
    		}
    		json.append(']');
    	}
    	json.append("]}");
    }
    
    public int sendRequest(boolean throwExc, boolean printJson) throws Exception {
    	int lg = json.length();
    	try {
	    	json.append(']');
	    	//byte[] content = URLEncoder.encode(json.toString(), "UTF-8").getBytes(UTF_8);
	    	byte[] content = json.toString().getBytes(UTF_8);
	    	if (printJson) {
		    	System.err.println("----");
		    	System.err.println(json);
		    	System.err.println("----");
	    	}
	    	
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
	 
			con.setRequestMethod("POST");
			//con.setRequestProperty("User-Agent", "InfluxDB-jvm");
	 
			// Send post request
			con.setDoOutput(true);
			OutputStream wr = con.getOutputStream();
			wr.write(content);
			wr.flush();
			wr.close();
	 
			int responseCode = con.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) {
				// ignore Response content
				con.getInputStream().close();
				resetRequest();
			} else if (throwExc) {
				throw new IOException("Server returned HTTP response code: " + responseCode +"for URL: "+ url + " with content :'" + con.getResponseMessage() +"'");
			}
			return responseCode;
    	} catch(Exception exc) {
    		json.setLength(lg);
    		throw exc;
    	}
    }
}
