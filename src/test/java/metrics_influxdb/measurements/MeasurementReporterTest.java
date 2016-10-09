package metrics_influxdb.measurements;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer.Context;
import metrics_influxdb.SortedMaps;
import metrics_influxdb.api.measurements.MetricMeasurementTransformer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static metrics_influxdb.SortedMaps.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class MeasurementReporterTest {
	private ListInlinerSender sender;
	private MetricRegistry registry;
	private MeasurementReporter reporter;

	@BeforeMethod
	public void init() {
		sender = new ListInlinerSender(100);
		registry = new MetricRegistry();
		reporter = new MeasurementReporter(sender, registry, null, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, Clock.defaultClock(), Collections.<String, String>emptyMap(), MetricMeasurementTransformer.NOOP);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneCounterGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String counterName = "c";
		Counter c = registry.counter(counterName);
		c.inc();
		reporter.report(SortedMaps.<String, Gauge>empty(), singleton(counterName, c), SortedMaps.<String, Histogram>empty(), SortedMaps.<String, Meter>empty(), SortedMaps.<String, Timer>empty());
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(counterName));
		assertThat(sender.getFrames().get(0), containsString("count=1i"));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneGaugeGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String gaugeName = "g";
		Gauge<Integer> g = new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 0;
			}
		};

		reporter.report(SortedMaps.<String, Gauge>singleton(gaugeName, g), SortedMaps.<String, Counter>empty(), SortedMaps.<String, Histogram>empty(), SortedMaps.<String, Meter>empty(), SortedMaps.<String, Timer>empty());
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(gaugeName));
		assertThat(sender.getFrames().get(0), containsString("value=0i"));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneMeterGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String meterName = "m";
		Meter meter = registry.meter(meterName);
		meter.mark();
		reporter.report(SortedMaps.<String, Gauge>empty(), SortedMaps.<String, Counter>empty(), SortedMaps.<String, Histogram>empty(), SortedMaps.singleton(meterName, meter), SortedMaps.<String, Timer>empty());

		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(meterName));

		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("one-minute="));
		assertThat(sender.getFrames().get(0), containsString("five-minute="));
		assertThat(sender.getFrames().get(0), containsString("fifteen-minute="));
		assertThat(sender.getFrames().get(0), containsString("mean-minute="));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneHistogramGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String histogramName = "h";
		Histogram histogram = registry.histogram(histogramName);
		histogram.update(0);
		reporter.report(SortedMaps.<String, Gauge>empty(), SortedMaps.<String, Counter>empty(), SortedMaps.singleton(histogramName, histogram), SortedMaps.<String, Meter>empty(), SortedMaps.<String, Timer>empty());

		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(histogramName));

		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("min="));
		assertThat(sender.getFrames().get(0), containsString("max="));
		assertThat(sender.getFrames().get(0), containsString("mean="));
		assertThat(sender.getFrames().get(0), containsString("std-dev="));
		assertThat(sender.getFrames().get(0), containsString("50-percentile="));
		assertThat(sender.getFrames().get(0), containsString("75-percentile="));
		assertThat(sender.getFrames().get(0), containsString("95-percentile="));
		assertThat(sender.getFrames().get(0), containsString("99-percentile="));
		assertThat(sender.getFrames().get(0), containsString("999-percentile="));
		assertThat(sender.getFrames().get(0), containsString("run-count="));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneTimerGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String timerName = "t";
		Timer meter = registry.timer(timerName);
		Context ctx = meter.time();

		try {
			Thread.sleep(20);
		} catch (InterruptedException ignored) {
		}

		ctx.stop();

		reporter.report(SortedMaps.<String, Gauge>empty(), SortedMaps.<String, Counter>empty(), SortedMaps.<String, Histogram>empty(), SortedMaps.<String, Meter>empty(), SortedMaps.singleton(timerName, meter));


		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(timerName));

		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("one-minute="));
		assertThat(sender.getFrames().get(0), containsString("five-minute="));
		assertThat(sender.getFrames().get(0), containsString("fifteen-minute="));
		assertThat(sender.getFrames().get(0), containsString("mean-minute="));
		assertThat(sender.getFrames().get(0), containsString("min="));
		assertThat(sender.getFrames().get(0), containsString("max="));
		assertThat(sender.getFrames().get(0), containsString("mean="));
		assertThat(sender.getFrames().get(0), containsString("std-dev="));
		assertThat(sender.getFrames().get(0), containsString("50-percentile="));
		assertThat(sender.getFrames().get(0), containsString("75-percentile="));
		assertThat(sender.getFrames().get(0), containsString("95-percentile="));
		assertThat(sender.getFrames().get(0), containsString("99-percentile="));
		assertThat(sender.getFrames().get(0), containsString("999-percentile="));
		assertThat(sender.getFrames().get(0), containsString("run-count="));
	}
}
