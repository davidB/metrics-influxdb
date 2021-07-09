package metrics_influxdb.api.measurements;

import com.codahale.metrics.Counter;
import metrics_influxdb.measurements.Measure;
import org.junit.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class RelativeCounterMetricsAdapterTest {

	@Test
	public void test() {
		RelativeCounterMetricsAdapter relativeCounterMetricsAdapter = new RelativeCounterMetricsAdapter();
		Counter counter = new Counter();
		counter.inc(999);
		final Measure first = relativeCounterMetricsAdapter.adapt("test", counter, new Measure("test"));

		assertThat(first.getValues().get("count")).isEqualTo("999i");
		assertThat(first.getValues().get("relativeCount")).isEqualTo("999i");

		counter.inc();
		Measure second = relativeCounterMetricsAdapter.adapt("test", counter, new Measure("test"));

		assertThat(second.getValues().get("count")).isEqualTo("1000i");
		assertThat(second.getValues().get("relativeCount")).isEqualTo("1i");
	}

}
