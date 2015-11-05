package metrics_influxdb.serialization.line;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import metrics_influxdb.measurements.Measure;
import metrics_influxdb.measurements.Measurement;
import metrics_influxdb.serialization.line.Inliner;

public class InlinerTest {
	private Inliner inliner;
	
	@Before
	public void init() {
		inliner = new Inliner();
	}
	
	@Test
	public void a_single_word_name_is_untouched() {
		String name = "cpu";
		Measurement m = new Measure(name, 1l);
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith(name));
	}

	@Test
	public void spaces_in_name_are_escaped() {
		String name = "cpu load";
		Measurement m = new Measure(name, 1l);
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("cpu\\ load"));
	}
	
	@Test
	public void comma_in_name_are_escaped() {
		String name = "cpu,01";
		Measurement m = new Measure(name, 1l);
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("cpu\\,01"));
	}
	
	@Test
	public void comma_and_spaces_in_name_are_escaped() {
		String name = "cpu load,01";
		Measurement m = new Measure(name, 1l);
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("cpu\\ load\\,01"));
	}
	
	@Test
	public void tags_are_part_of_the_measure_key() {
		String name = "cpu";
		Map<String, String> tags = new HashMap<>();
		tags.put("server", "127.0.0.1");
		tags.put("type", "prod");
		
		Measurement m = new Measure(name, tags, 0l, System.currentTimeMillis());
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("cpu,server=127.0.0.1,type=prod"));

	}
	
	@Test
	public void tags_keys_with_spaces_are_escaped() {
		String name = "cpu";
		Map<String, String> tags = new HashMap<>();
		tags.put("server ip", "127.0.0.1");
		
		Measurement m = new Measure(name, tags, 0l, System.currentTimeMillis());
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("cpu,server\\ ip=127.0.0.1"));
		
	}
	
	@Test
	public void tags_keys_with_commas_are_escaped() {
		String name = "cpu";
		Map<String, String> tags = new HashMap<>();
		tags.put("server,ip", "127.0.0.1");
		
		Measurement m = new Measure(name, tags, 0l, System.currentTimeMillis());
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("cpu,server\\,ip=127.0.0.1"));
		
	}
	
	@Test
	public void tags_values_with_spaces_are_escaped() {
		String name = "projection";
		Map<String, String> tags = new HashMap<>();
		tags.put("hero", "luke skywalker");
		
		Measurement m = new Measure(name, tags, 0l, System.currentTimeMillis());
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("projection,hero=luke\\ skywalker"));
		
	}
	
	@Test
	public void tags_keys_and_values_are_escaped() {
		String name = "projection";
		Map<String, String> tags = new HashMap<>();
		tags.put("main hero", "luke, skywalker");
		
		Measurement m = new Measure(name, tags, 0l, System.currentTimeMillis());
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("projection,main\\ hero=luke\\,\\ skywalker"));
	}
	
	@Test
	public void given_timestamp_is_used() {
		long time = System.currentTimeMillis();
		Measurement m = new Measure("cpu", 0l, time);
		
		String output = inliner.inline(m);
		
		assertThat(output, endsWith(""+time));
	}
	
	@Test
	public void a_timestamp_is_generated_when_none_is_provided() {
		Long initialTime = System.currentTimeMillis();
		
		Measurement m = new Measure("cpu", 0l);
		
		String output = inliner.inline(m);
		
		Pattern p = Pattern.compile(".*\\s([0-9]+)");
		Matcher matcher = p.matcher(output);
		assertTrue("generated output ends with a number", matcher.matches());
		
		Long generatedTimestamp = Long.valueOf(matcher.group(1));
		
		assertThat(generatedTimestamp, greaterThanOrEqualTo(initialTime));
	}
	
	@Test
	public void a_value_nammed_value_is_generated_for_single_float_value() {
		Measurement m = new Measure("cpu", 80.0f);
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=80.0"));
	}
	
	@Test
	public void a_value_nammed_value_is_generated_for_single_double_value() {
		Measurement m = new Measure("cpu", 50.03d);
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=50.03"));
	}
	
	@Test
	public void a_value_nammed_value_is_generated_for_single_integer_value() {
		Measurement m = new Measure("cpu", 50);
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=50i"));
	}
	
	@Test
	public void a_value_nammed_value_is_generated_for_single_long_value() {
		Measurement m = new Measure("cpu", 75l);
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=75i"));
	}
	
	@Test
	public void a_value_nammed_value_is_generated_for_single_string_value() {
		Measurement m = new Measure("cpu", "high");
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=\"high\""));
	}
	
	@Test
	public void a_value_nammed_value_is_generated_for_single_true_value() {
		Measurement m = new Measure("multi-core", Boolean.TRUE);
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=true"));
	}
	
	@Test
	public void a_value_nammed_value_is_generated_for_single_false_value() {
		Measurement m = new Measure("multi-core", Boolean.FALSE);
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=false"));
	}
	
	@Test
	public void comma_or_spaces_in_string_values_are_not_escaped() {
		Measurement m = new Measure("multi-core", "do not, escape");
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=\"do not, escape\""));
	}
	
	@Test
	public void value_names_with_spaces_and_comma_are_escaped() {
		Measurement m = new Measure("cpu", Boolean.FALSE);
		
		String output = inliner.inline(m);
		
		assertThat(output, containsString("value=false"));
	}
	
	@Test
	public void values_can_be_added_fluently() {
		Measurement m = new Measure("cpu").addValue("load", 10).addValue("alert", true).addValue("reason", "value above maximum threshold");
		
		String output = inliner.inline(m);
		
		assertThat(output, startsWith("cpu alert=true,load=10i,reason=\"value above maximum threshold\""));
	}
	
	@Test
	public void multiple_measurements_are_separated_by_cr_when_inlined() {
		Measurement m = new Measure("load", 10);
		
		String output = inliner.inline(Arrays.asList(m, m, m));
		
		String[] lines = output.split("\n");
		
		assertThat(lines.length, is(3));
		assertThat(lines[0], is(inliner.inline(m)));
		assertThat(lines[1], is(inliner.inline(m)));
		assertThat(lines[2], is(inliner.inline(m)));
	}
}
