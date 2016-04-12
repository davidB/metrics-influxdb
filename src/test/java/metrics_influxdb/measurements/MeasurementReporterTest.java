package metrics_influxdb.measurements;

import static metrics_influxdb.SortedMaps.empty;
import static metrics_influxdb.SortedMaps.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsString;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import metrics_influxdb.api.measurements.MetricMeasurementTransformer;
import metrics_influxdb.measurements.MeasurementReporter;

public class MeasurementReporterTest {
	private ListInlinerSender sender;
	private MetricRegistry registry;
	private MeasurementReporter reporter;

	@BeforeMethod
	public void init() {
		sender = new ListInlinerSender(100);
		registry = new MetricRegistry();
		reporter = new MeasurementReporter(sender, registry, null, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, Clock.defaultClock(), Collections.emptyMap(), MetricMeasurementTransformer.NOOP);
	}

	@Test
	public void reportingOneCounterGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String counterName = "c";
		Counter c = registry.counter(counterName);
		c.inc();
		reporter.report(empty(), singleton(counterName, c), empty(), empty(), empty());
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(counterName));
		assertThat(sender.getFrames().get(0), containsString("count=1i"));
	}

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

		reporter.report(singleton(gaugeName, g), empty(), empty(), empty(), empty());
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(gaugeName));
		assertThat(sender.getFrames().get(0), containsString("value=0i"));
	}

	@Test
	public void reportingOneMeterGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String meterName = "m";
		Meter meter = registry.meter(meterName);
		meter.mark();
		reporter.report(empty(), empty(), empty(), singleton(meterName, meter), empty());
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith(meterName));

		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("one-minute="));
		assertThat(sender.getFrames().get(0), containsString("five-minute="));
		assertThat(sender.getFrames().get(0), containsString("fifteen-minute="));
		assertThat(sender.getFrames().get(0), containsString("mean-minute="));
	}

	@Test
	public void reportingOneHistogramGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String histogramName = "h";
		Histogram histogram = registry.histogram(histogramName);
		histogram.update(0);
		reporter.report(empty(), empty(), singleton(histogramName, histogram), empty(), empty());
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

	@Test
	public void reportingOneTimerGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		String timerName = "t";
		Timer meter = registry.timer(timerName);
		Context ctx = meter.time();

		try {
			Thread.sleep(20);
		} catch (InterruptedException e) {
		}

		ctx.stop();

		reporter.report(empty(), empty(), empty(), empty(), singleton(timerName, meter));

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
