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
package metrics_influxdb;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
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
public class Influxdb {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  public static char toTimePrecision(TimeUnit t) {
    switch (t) {
    case SECONDS:
      return 's';
    case MILLISECONDS:
      return 'm';
    case MICROSECONDS:
      return 'u';
    default:
      throw new IllegalArgumentException("time precision should be SECONDS or MILLISECONDS or MICROSECONDS");
    }
  }

  public final URL url;
  /** true => to print Json on System.err */
  public boolean debugJson = false;
  
  /**
   * @throws IOException If the URL is malformed
   */
  public Influxdb(String host, int port, String database, String username, String password, TimeUnit timePrecision) throws IOException  {
    try {
      String encodedUsername = URLEncoder.encode(username, UTF_8.name());
      this.url = new URL("http", host, port, "/db/" + database + "/series?u=" + encodedUsername + "&p=" + password + 
          "&time_precision=" + toTimePrecision(timePrecision));
    } catch (UnsupportedEncodingException e) {
      // All JVMs are required to support UTF-8, so if this happens it's a programming error,
      // so don't require the user to catch it.
      throw new RuntimeException(e);
    }
  }

  public Influxdb(URL url) {
    this.url = url;
  }

  public int sendRequest(String json, boolean throwExc, boolean printJson) throws IOException {

    // byte[] content = URLEncoder.encode(json.toString(),
    // "UTF-8").getBytes(UTF_8);
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
