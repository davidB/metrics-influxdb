package metrics_influxdb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Arrays;

public class InfluxdbUdp implements Influxdb {
	protected final ArrayList<JsonBuilder> jsonBuilders;
	private final String host;
	private final int port;
	public boolean debugJson = false;

	public InfluxdbUdp(String host, int port) {
		jsonBuilders = new ArrayList<>();

		this.host = host;
		this.port = port;
	}

	@Override
	public void resetRequest() {
		jsonBuilders.clear();
	}

	@Override
	public boolean hasSeriesData() {
		return !jsonBuilders.isEmpty();
	}

	@Override
	public boolean shouldIncludeTimestamps() {
		return false;
	}

	@Override
	public void appendSeries(String namePrefix, String name, String nameSuffix, SeriesData data) {
		JsonBuilderDefault jsonBuilder = new JsonBuilderDefault();
		jsonBuilder.reset();
		jsonBuilder.appendSeries(namePrefix, name, nameSuffix, data);
		jsonBuilders.add(jsonBuilder);
	}

	@Override
	public int sendRequest(boolean throwExc, boolean printJson) throws Exception {
		DatagramChannel channel = null;

		try {
			channel = DatagramChannel.open();
			InetSocketAddress socketAddress = new InetSocketAddress(host, port);

			for (JsonBuilder builder : jsonBuilders) {
				String json = builder.toJsonString();

				if (printJson || debugJson) {
					System.out.println(json);
				}

				ByteBuffer buffer = ByteBuffer.wrap(json.getBytes());
				channel.send(buffer, socketAddress);
				buffer.clear();
			}
		} catch (Exception e) {
			if (throwExc) {
				throw e;
			}
		} finally {
			if (channel != null) {
				channel.close();
			}
		}

		return 0;
	}

	private static void removeColumn(String name, Object[] columns, Object[][] points) {
		int columnIndex = -1;

		for (int i = 0; i < columns.length; ++i) {
			if (columns[i].equals(name)) {
				columnIndex = i;
				break;
			}
		}

		if (columnIndex == -1) {
			return;
		}

		ArrayList<Object> cols = new ArrayList<>(Arrays.asList(columns));
		cols.remove(0);
		columns = cols.toArray();
	}
}
