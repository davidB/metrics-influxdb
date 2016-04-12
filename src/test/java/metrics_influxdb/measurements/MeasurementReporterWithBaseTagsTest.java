package metrics_influxdb.measurements;

import static metrics_influxdb.SortedMaps.empty;
import static metrics_influxdb.SortedMaps.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsString;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;
import static org.hamcrest.MatcherAssert.assertThat;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import metrics_influxdb.api.measurements.MetricMeasurementTransformer;

public class MeasurementReporterWithBaseTagsTest {
	private ListInlinerSender sender = new ListInlinerSender(100);
	private MetricRegistry registry = new MetricRegistry();

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
		reporter.report(empty(), singleton(counterName, c), empty(), empty(), empty());
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(counterName));
		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("server=icare"));
		assertThat(sender.getFrames().get(0), startsWith(String.format("%s,%s=%s", counterName, serverKey, serverName)));

		reporter.close();
	}
}
