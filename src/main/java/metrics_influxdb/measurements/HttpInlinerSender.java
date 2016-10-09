package metrics_influxdb.measurements;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import metrics_influxdb.HttpInfluxdbProtocol;
import metrics_influxdb.misc.Miscellaneous;
import metrics_influxdb.serialization.line.Inliner;

public class HttpInlinerSender extends QueueableSender {
	private final static Logger LOGGER = LoggerFactory.getLogger(HttpInlinerSender.class);
	private static int MAX_MEASURES_IN_SINGLE_POST = 5000;
	private final URL writeURL;
	private final Inliner inliner;

	public HttpInlinerSender(HttpInfluxdbProtocol protocol) {
		super(MAX_MEASURES_IN_SINGLE_POST);
		URL toJoin;

		inliner = new Inliner(TimeUnit.MILLISECONDS);

		try {
			if (protocol.secured) {
				toJoin = new URL(protocol.scheme, protocol.host, protocol.port, "/write?precision=ms&db=" + Miscellaneous.urlEncode(protocol.database) + "&u="
						+ Miscellaneous.urlEncode(protocol.user) + "&p=" + Miscellaneous.urlEncode(protocol.password));
			} else {
				toJoin = new URL(protocol.scheme, protocol.host, protocol.port, "/write?precision=ms&db=" + Miscellaneous.urlEncode(protocol.database));
			}
		} catch (MalformedURLException | UnsupportedEncodingException e) {
			toJoin = null;
		}

		writeURL = toJoin;
	}

	@Override
	protected boolean doSend(Collection<Measure> measures) {
		if (measures.isEmpty()) {
			return true;
		}

		HttpURLConnection con = null;
		try {
			con = (HttpURLConnection) writeURL.openConnection();
			con.setRequestMethod("POST");
			con.setConnectTimeout(Long.valueOf(TimeUnit.SECONDS.toMillis(2)).intValue());
			con.setReadTimeout(Long.valueOf(TimeUnit.SECONDS.toMillis(2)).intValue());

			// Send post request
			con.setDoOutput(true);
			OutputStream wr = con.getOutputStream();
			String measuresAsString = inliner.inline(measures);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Measures being sent:\n{}", measuresAsString);
			}
			wr.write(measuresAsString.getBytes(Miscellaneous.UTF8));

			wr.flush();
			wr.close();

			int responseCode = con.getResponseCode();

			switch (responseCode) {
			case HttpURLConnection.HTTP_NO_CONTENT:
				LOGGER.debug("{} Measures sent to {}://{}:{}", measures.size(), writeURL.getProtocol(), writeURL.getHost(), writeURL.getPort());
				break;
			case HttpURLConnection.HTTP_OK:
				LOGGER.info("{} Measures sent to {}://{}:{} but not saved by infludb, reason:\n{}", measures.size(), writeURL.getProtocol(), writeURL.getHost(), writeURL.getPort(), Miscellaneous.readFrom(con.getInputStream()));
				break;
			default:
				LOGGER.info("failed to send {} Measures to {}://{}:{}, HTTP CODE received: {}\n", measures.size(), writeURL.getProtocol(), writeURL.getHost(), writeURL.getPort(), responseCode,  Miscellaneous.readFrom(con.getInputStream()));
				break;
			}

			return true;
		} catch (IOException e) {
			// Here the influxdb is potentially temporary unreachable
			// we do not clear held measures so that we'll eb able to retry to post them
			LOGGER.warn("couldn't sent metrics to {}://{}:{}, reason: {}", writeURL.getProtocol(), writeURL.getHost(), writeURL.getPort(), e.getMessage(), e);
		} finally {
			// cleanup connection streams
			if (con != null) {
				try {
					con.getInputStream().close();
				} catch (Exception ignore) {
					// ignore
				}
			}
		}

		return false;
	}
}
