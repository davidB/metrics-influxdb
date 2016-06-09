package metrics_influxdb.measurements;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Clock;

import metrics_influxdb.misc.Miscellaneous;

public class Measure {
	private String name;
	private Map<String, String> tags;
	private Map<String, String> values;
	private long timestamp;

	public Measure(String name) {
		this(name, (Map<String, String>)null, (Map<String, String>)null, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, Map<String, String> values, long timestamp) {
		super();
		this.name = name;
		this.tags = new HashMap<String, String>();
		this.values = new HashMap<String, String>();
		this.timestamp = timestamp;

		if (tags != null) {
			this.tags.putAll(tags);
		}
		if (values != null) {
			this.values.putAll(values);
		}
	}

	public Measure(String name, Map<String, String> tags, long value) {
		this(name, tags, value, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, long value, long timestamp) {
		this(name, tags, Collections.singletonMap("value", value+"i"), timestamp);
	}

	public Measure(String name, long value, long timestamp) {
		this(name, Collections.<String, String>emptyMap(), value, timestamp);
	}

	public Measure(String name, long value) {
		this(name, value, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, int value) {
		this(name, tags, Long.valueOf(value), Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, int value, long timestamp) {
		this(name, tags, Long.valueOf(value), timestamp);
	}

	public Measure(String name, int value, long timestamp) {
		this(name, null, Long.valueOf(value), timestamp);
	}

	public Measure(String name, int value) {
		this(name, Long.valueOf(value), Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, double value) {
		this(name, tags, value, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, double value, long timestamp) {
		this(name, tags, Collections.singletonMap("value", ""+value), timestamp);
	}

	public Measure(String name, double value, long timestamp) {
		this(name, Collections.<String, String>emptyMap(), value, timestamp);
	}

	public Measure(String name, double value) {
		this(name, value, Clock.defaultClock().getTime());
	}	

	public Measure(String name, Map<String, String> tags, float value) {
		this(name, tags, Double.valueOf(value), Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, float value, long timestamp) {
		this(name, tags, Double.valueOf(value), timestamp);
	}

	public Measure(String name, float value, long timestamp) {
		this(name, null, Double.valueOf(value), timestamp);
	}

	public Measure(String name, float value) {
		this(name, Double.valueOf(value), Clock.defaultClock().getTime());
	}	

	public Measure(String name, Map<String, String> tags, String value) {
		this(name, tags, value, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, String value, long timestamp) {
		this(name, tags, Collections.singletonMap("value", asStringValue(value)), timestamp);
	}

	public Measure(String name, String value, long timestamp) {
		this(name, null, value, timestamp);
	}

	public Measure(String name, String value) {
		this(name, value, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, boolean value) {
		this(name, tags, value, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, boolean value, long timestamp) {
		this(name, tags, Collections.singletonMap("value", ""+value), timestamp);
	}

	public Measure(String name, boolean value, long timestamp) {
		this(name, null, value, timestamp);
	}

	public Measure(String name, boolean value) {
		this(name, value, Clock.defaultClock().getTime());
	}

	private static String asStringValue(String value) {
		return "\"" + Miscellaneous.escape(value, '"') + "\"";
	}

	public String getName() {
		return name;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public Map<String, String> getValues() {
		return values;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setTags(Map<String, String> tags) {
		this.tags.clear();
		if (tags != null) {
			this.tags.putAll(tags);
		}
	}

	public void setValues(Map<String, String> values) {
		this.values.clear();
		if (values != null) {
			this.values.putAll(values);
		}
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Measure timestamp(long timestamp) {
		setTimestamp(timestamp);
		return this;
	}

	public Measure addTag(String tagKey, String tagValue) {
		tags.put(tagKey, tagValue);
		return this;
	}
	public Measure addTag(Map<String, String> tags) {
		this.tags.putAll(tags);
		return this;
	}
	public Measure addValue(String key, String value) {
		values.put(key, asStringValue(value));
		return this;
	}
	public Measure addValue(String key, float value) {
		return addValue(key, Double.valueOf(value));
	}
	public Measure addValue(String key, double value) {
		if (!((Double.isNaN(value)) || Double.isInfinite(value))) {
			values.put(key, "" + value);
		}
		return this;
	}
	public Measure addValue(String key, int value) {
		return addValue(key, Long.valueOf(value));
	}
	public Measure addValue(String key, long value) {
		values.put(key, value+"i");
		return this;
	}
	public Measure addValue(String key, boolean value) {
		values.put(key, ""+value);
		return this;
	}
}
