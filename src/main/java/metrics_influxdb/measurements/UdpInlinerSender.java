package metrics_influxdb.measurements;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import metrics_influxdb.UdpInfluxdbProtocol;
import metrics_influxdb.misc.Miscellaneous;
import metrics_influxdb.serialization.line.Inliner;

public class UdpInlinerSender extends QueueableSender {
	private final static Logger LOGGER = LoggerFactory.getLogger(UdpInlinerSender.class);
	private static int MAX_MEASURES_IN_SINGLE_POST = 5000;
	private final Inliner inliner;
	private final InetSocketAddress serverAddress;

	public UdpInlinerSender(UdpInfluxdbProtocol protocol) {
		super(MAX_MEASURES_IN_SINGLE_POST);
		inliner = new Inliner(TimeUnit.NANOSECONDS);
		serverAddress = new InetSocketAddress(protocol.host, protocol.port);
	}

	@Override
	protected boolean doSend(Collection<Measure> measures) {
		if (measures.isEmpty()) {
			return true;
		}

		DatagramChannel channel = null;
		try {
			channel = DatagramChannel.open();
		} catch (IOException e) {
			LOGGER.error("failed open udp channel", e);
			return false;
		}
		Iterator<Measure> measuresIterator = measures.iterator();
		int errorCounter = 0;
		int successCounter = 0;
		while(measuresIterator.hasNext()) {
			String measuresAsString = inliner.inline(measuresIterator.next());
			try {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Measure being sent:\n{}", measuresAsString);
				}

				ByteBuffer buffer = ByteBuffer.wrap(measuresAsString.getBytes(Miscellaneous.UTF8));
				channel.send(buffer, serverAddress);
				successCounter++;
			} catch (Throwable e) {
				errorCounter++;
			}
		}
		LOGGER.debug("{} Measures sent to UDP[{}:{}]; successes: {}, failures: {}",
				measures.size(), serverAddress.getHostString(), serverAddress.getPort(), successCounter, errorCounter);
		try {
			channel.close();
		} catch (IOException e) {
			LOGGER.error("failed close udp channel", e);
		}
		return successCounter > 0;
	}
}
