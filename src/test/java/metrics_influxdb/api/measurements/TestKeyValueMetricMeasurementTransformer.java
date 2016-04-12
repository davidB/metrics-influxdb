package metrics_influxdb.api.measurements;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestKeyValueMetricMeasurementTransformer {
	private KeyValueMetricMeasurementTransformer keyValueTransformer = new KeyValueMetricMeasurementTransformer();

	@Test
	public void aSimpleMetricGeneratesEmptyTags() {
		String metricName = "cpu_load";

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet(), empty());
	}

	@Test
	public void aSimpleMetricGeneratesIsoMeasurementName() {
		String metricName = "cpu_load";

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is(metricName));
	}

	@Test
	public void aMetricWith2SubStringsGeneratesEmptyTags() {
		String metricName = "cores.cpu_load";

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet(), empty());
	}

	@Test
	public void aMetricWith2SubStringsGeneratesIsoMeasurementName() {
		String metricName = "cores.cpu_load";

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is(metricName));
	}

	@Test
	public void aMetricWithEven2NPlus1SubStringsGeneratesNTags() {
		String metricName = "server.actarus.cpu_load";

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet().size(), is(1));
		assertThat(tags, hasEntry("server", "actarus"));
	}

	@Test
	public void aMetricWithEven2NPlus1SubStringsGeneratesMeasurementNameWithLastSubString() {
		String metricName = "server.actarus.cpu_load";

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is("cpu_load"));
	}

	@Test
	public void aMetricWithGeneratedEven2NPlus1SubStringsGeneratesNTags() {
		String baseMetricName = "metric";
		List<String> subStrings = new ArrayList<>();

		int n = 10;
		for (int i = 0; i < n; i++) {
			subStrings.add("key" + i);
			subStrings.add("value" + i);
		}
		subStrings.add(baseMetricName);

		String metricName = String.join(".", subStrings.toArray(new String[]{}));

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet().size(), is(n));
	}

	@Test
	public void aMetricWithGeneratedEven2NSubStringsGeneratesNMinusOneTags() {
		String baseMetricName = "metric.name";
		List<String> subStrings = new ArrayList<>();

		int nMinusOne = 9;
		for (int i = 0; i < nMinusOne; i++) {
			subStrings.add("key" + i);
			subStrings.add("value" + i);
		}
		subStrings.add(baseMetricName);

		String metricName = String.join(".", subStrings.toArray(new String[]{}));

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet().size(), is(nMinusOne));
	}

	@Test
	public void aMetricWithGeneratedEven2NPlus1SubStringsGeneratesMeasurementNameWithLastSubString() {
		String baseMetricName = "metric";
		List<String> subStrings = new ArrayList<>();

		int n = 10;
		for (int i = 0; i < n; i++) {
			subStrings.add("key" + i);
			subStrings.add("value" + i);
		}
		subStrings.add(baseMetricName);

		String metricName = String.join(".", subStrings.toArray(new String[]{}));

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is(baseMetricName));
	}

	@Test
	public void aMetricWithGeneratedEven2NSubStringsGeneratesMeasurementNameWithLast2SubString() {
		String baseMetricName = "metric.name";
		List<String> subStrings = new ArrayList<>();

		int nMinusOne = 9;
		for (int i = 0; i < nMinusOne; i++) {
			subStrings.add("key" + i);
			subStrings.add("value" + i);
		}
		subStrings.add(baseMetricName);

		String metricName = String.join(".", subStrings.toArray(new String[]{}));

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is(baseMetricName));
	}
}
