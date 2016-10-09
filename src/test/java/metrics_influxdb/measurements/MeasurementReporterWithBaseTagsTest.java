package metrics_influxdb.measurements;

import com.codahale.metrics.*;
import metrics_influxdb.SortedMaps;
import metrics_influxdb.api.measurements.MetricMeasurementTransformer;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static metrics_influxdb.SortedMaps.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class MeasurementReporterWithBaseTagsTest {
	private ListInlinerSender sender = new ListInlinerSender(100);
	private MetricRegistry registry = new MetricRegistry();

	@SuppressWarnings("rawtypes")
	@Test
	public void generatedMeasurementContainsBaseTags() {
		String serverKey = "server";
		String serverName = "icare";
		Map<String, String> baseTags = new HashMap<>();
		baseTags.put(serverKey, serverName);

		MeasurementReporter reporter = new MeasurementReporter(sender, registry, null, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, Clock.defaultClock(), baseTags, MetricMeasurementTransformer.NOOP);
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String counterName = "c";
		Counter c = registry.counter(counterName);
		c.inc();
		reporter.report(SortedMaps.<String, Gauge>empty(), singleton(counterName, c), SortedMaps.<String, Histogram>empty(), SortedMaps.<String, Meter>empty(), SortedMaps.<String, Timer>empty());
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(counterName));
		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("server=icare"));
		assertThat(sender.getFrames().get(0), startsWith(String.format("%s,%s=%s", counterName, serverKey, serverName)));

		reporter.close();
	}
}
