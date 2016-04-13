package sandbox;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import metrics_influxdb.InfluxdbReporter;
import metrics_influxdb.UdpInfluxdbProtocol;
import metrics_influxdb.v08.ReporterV08;

import java.util.concurrent.TimeUnit;

public class InfluxdbUdpSandbox {
	private static final String ENV_INFLUX_HOST = "INFLUXDB_HOST";
	private static final String ENV_INFLUX_PORT = "INFLUXDB_PORT";
	private static final String ENV_REGISTRY_PREFIX = "REGISTRY_PREFIX";

	public static void main(String[] args) {
		ReporterV08 reporter = null;
		try {
			final MetricRegistry registry = new MetricRegistry();
			reporter = getInfluxdbReporter(registry);
			reporter.start(3, TimeUnit.SECONDS);
			final Counter counter = registry.counter(MetricRegistry.name("test", "counter"));
			for (int i = 0; i < 1; ++i) {
				counter.inc();
				Thread.sleep(Math.round(Math.random()) * 1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			if (reporter != null) {
				reporter.report();
				reporter.stop();
			}
		}
	}

	private static ReporterV08 getInfluxdbReporter(MetricRegistry registry) throws Exception {
		return (ReporterV08) InfluxdbReporter
				.forRegistry(registry)
				.protocol(new UdpInfluxdbProtocol(getEnv(ENV_INFLUX_HOST), Integer.parseInt(getEnv(ENV_INFLUX_PORT))))
				.prefixedWith(getEnv(ENV_REGISTRY_PREFIX, "test"))
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS)
				.filter(MetricFilter.ALL)
				.build();
	}

	private static String getEnv(String key) throws Exception {
		String envVal = System.getenv(key);

		if (envVal == null) {
			throw new Exception("missing environment variable: "+key);
		}

		return envVal;
	}

	private static String getEnv(String key, String ifMissing) {
		try {
			return getEnv(key);
		} catch (Exception e) {
			return ifMissing;
		}
	}
}
