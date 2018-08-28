package metrics_influxdb.misc;

import metrics_influxdb.HttpInfluxdbProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public final class HttpDatabaseCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpDatabaseCreator.class);

    /**
     * Runs a CREATE DATABASE against influx. Influx does not do any action if the database already exists.
     */
    public static void run(HttpInfluxdbProtocol protocol) {
        URL toJoin;

        try {
            if (protocol.secured) {
                toJoin = new URL(protocol.scheme, protocol.host, protocol.port, "/query&u="
                        + Miscellaneous.urlEncode(protocol.user) + "&p=" + Miscellaneous.urlEncode(protocol.password));
            } else {
                toJoin = new URL(protocol.scheme, protocol.host, protocol.port, "/query");
            }
        } catch (MalformedURLException | UnsupportedEncodingException e) {
            toJoin = null;
        }
        String database = protocol.database;
        URL queryUrl = toJoin;
        try {
            HttpURLConnection con;

            con = (HttpURLConnection) queryUrl.openConnection();
            con.setRequestMethod("GET");
            con.setConnectTimeout(Long.valueOf(TimeUnit.SECONDS.toMillis(2)).intValue());
            con.setReadTimeout(Long.valueOf(TimeUnit.SECONDS.toMillis(2)).intValue());

            con.setDoOutput(true);
            OutputStream wr = con.getOutputStream();
            wr.write(("q=CREATE DATABASE " + database).getBytes());
            wr.flush();
            wr.close();
        } catch (IOException e) {
            LOGGER.warn("Tried to create database, but failed.", e);
        }
    }
}
