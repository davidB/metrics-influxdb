//	metrics-influxdb
//
//	Written in 2014 by David Bernard <dbernard@novaquark.com>
//
//	[other author/contributor lines as appropriate]
//
//	To the extent possible under law, the author(s) have dedicated all copyright and
//	related and neighboring rights to this software to the public domain worldwide.
//	This software is distributed without any warranty.
//
//	You should have received a copy of the CC0 Public Domain Dedication along with
//	this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
package metrics_influxdb.v08;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * A client to send data to a InfluxDB server via HTTP protocol.
 *
 * The usage :
 *
 * <pre>
 *   Influxdb influxdb = new Influxdb(...);
 *
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
public class InfluxdbHttp implements Influxdb {
	private static final Charset UTF_8 = Charset.forName("UTF-8");

	public static String toTimePrecision(TimeUnit t) {
		switch (t) {
		case SECONDS:
			return "s";
		case MILLISECONDS:
			return "ms";
		case MICROSECONDS:
			return "u";
		default:
			throw new IllegalArgumentException("time precision should be SECONDS or MILLISECONDS or MICROSECONDS");
		}
	}

	public final URL url;
	/** if true then print Json on System.err */
	public boolean debugJson = false;
	public JsonBuilder jsonBuilder = new JsonBuilderDefault();
	/**
	 * Constructor with the InfluxDB time_precision parameter set to TimeUnit.MILLISECONDS
	 * @throws IOException If the URL is malformed
	 */
	public InfluxdbHttp(String host, int port, String database, String username, String password) throws Exception  {
		this("http", host, port, "", database, username, password, TimeUnit.MILLISECONDS);
	}

	/**
	 * Constructor with the InfluxDB time_precision parameter set to TimeUnit.MILLISECONDS
	 * @throws IOException If the URL is malformed
	 */
	public InfluxdbHttp(String host, int port, String path, String database, String username, String password) throws Exception  {
		this("http", host, port, path, database, username, password, TimeUnit.MILLISECONDS);
	}
	/**
	 * @param timePrecision The precision of the epoch time that is sent to the server,
	 *                      should be TimeUnit.MILLISECONDS unless you are using a custom Clock
	 *                      that does not return milliseconds epoch time for getTime()
	 * @throws IOException If the URL is malformed
	 */
	public InfluxdbHttp(String host, int port, String database, String username, String password, TimeUnit timePrecision) throws Exception  {
		this("http", host, port, database, username, password, timePrecision);
	}

	/**
	 * @param timePrecision The precision of the epoch time that is sent to the server,
	 *                      should be TimeUnit.MILLISECONDS unless you are using a custom Clock
	 *                      that does not return milliseconds epoch time for getTime()
	 * @throws IOException If the URL is malformed
	 */
	public InfluxdbHttp(String scheme, String host, int port, String database, String username, String password, TimeUnit timePrecision) throws Exception  {
		this(scheme, host, port, "", database, username, password, timePrecision);
	}

	/**
	 * @param timePrecision The precision of the epoch time that is sent to the server,
	 *                      should be TimeUnit.MILLISECONDS unless you are using a custom Clock
	 *                      that does not return milliseconds epoch time for getTime()
	 * @throws IOException If the URL is malformed
	 */
	public InfluxdbHttp(String scheme, String host, int port, String path, String database, String username, String password, TimeUnit timePrecision) throws Exception  {
		this.url = new URL(scheme, host, port,
				path + "/db/" + database
				+ "/series?u=" + URLEncoder.encode(username, UTF_8.name())
				+ "&p=" + password
				+ "&time_precision=" + toTimePrecision(timePrecision)
				);
	}

	/**
	 * Returns true if the pending request has metrics to report.
	 */
	public boolean hasSeriesData() {
		return jsonBuilder.hasSeriesData();
	}

	@Override
	public long convertTimestamp(long timestamp) {
		return timestamp;
	}

	/**
	 * Forgot previously appendSeries.
	 */
	public void resetRequest() {
		jsonBuilder.reset();
	}

	public void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points) {
		jsonBuilder.appendSeries(namePrefix, name, nameSuffix, columns, points);
	}
	public int sendRequest(boolean throwExc, boolean printJson) throws Exception {
		String json = jsonBuilder.toJsonString();

		if (printJson || debugJson) {
			System.err.println("----");
			System.err.println(json);
			System.err.println("----");
		}

		HttpURLConnection con = (HttpURLConnection) url.openConnection();

		con.setRequestMethod("POST");
		// con.setRequestProperty("User-Agent", "InfluxDB-jvm");

		// Send post request
		con.setDoOutput(true);
		OutputStream wr = con.getOutputStream();
		wr.write(json.getBytes(UTF_8));
		wr.flush();
		wr.close();

		int responseCode = con.getResponseCode();
		if (responseCode == HttpURLConnection.HTTP_OK) {
			// ignore Response content
			con.getInputStream().close();
		} else if (throwExc) {
			throw new IOException("Server returned HTTP response code: " + responseCode + "for URL: " + url + " with content :'" + con.getResponseMessage() + "'");
		}
		return responseCode;
	}
}
