package metrics_influxdb;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;

public class InfluxdbUdp implements Influxdb {
	protected final ArrayList<JsonBuilder> jsonBuilders;
	private final String host;
	private final int port;
	public boolean debugJson = false;
	private String version;
	private String database;

	public InfluxdbUdp(String host, int port) {
		jsonBuilders = new ArrayList<>();

		this.host = host;
		this.port = port;
	}

	public InfluxdbUdp(String host, int port, String database, String version) {
		this(host, port);
		this.database = database;
		this.version = version;
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
	public long convertTimestamp(long timestamp) {
		return timestamp / 1000; // when sending timestamps over udp, they must be in seconds https://github.com/influxdb/influxdb/issues/841
	}

	@Override
	public void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points) {
		JsonBuilder jsonBuilder = getJsonBuilder();
		jsonBuilder.reset();
		jsonBuilder.appendSeries(namePrefix, name, nameSuffix, columns, points);
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

	private JsonBuilder getJsonBuilder(){
		if("0.9".equals(version)){
			return new JsonBuilderV9(this.database, "");
		}
		return  new JsonBuilderDefault();
	}
}
