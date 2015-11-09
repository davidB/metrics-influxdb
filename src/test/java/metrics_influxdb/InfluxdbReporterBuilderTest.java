package metrics_influxdb;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import metrics_influxdb.InfluxdbReporter.Builder;
import metrics_influxdb.api.measurements.MetricMeasurementTransformer;
import metrics_influxdb.api.protocols.HttpInfluxdbProtocol;
import metrics_influxdb.api.protocols.InfluxdbProtocols;

public class InfluxdbReporterBuilderTest {
    private MetricRegistry registry;
    
    @Before
    public void init() {
        registry = new MetricRegistry();
    }

    @Test
    public void builder_api_with_default_values() {
        ScheduledReporter reporter = InfluxdbReporter.forRegistry(registry).build();
        
        assertThat(reporter, notNullValue());
    }
    
    @Test
    public void check_defaults_from_builder() {
        Builder builder = InfluxdbReporter.forRegistry(registry);
        
        assertThat(builder, notNullValue());
        
        // Check protocols defaults
        assertThat(builder.protocol, instanceOf(HttpInfluxdbProtocol.class));
        
        HttpInfluxdbProtocol httpProtocol = (HttpInfluxdbProtocol) builder.protocol;
        assertThat(httpProtocol.getHost(), is(HttpInfluxdbProtocol.DEFAULT_HOST));
        assertThat(httpProtocol.getPort(), is(HttpInfluxdbProtocol.DEFAULT_PORT));
        assertThat(httpProtocol.getDatabase(), is(HttpInfluxdbProtocol.DEFAULT_DATABASE));
        assertFalse(httpProtocol.isSecured());

        // other defaults
        assertThat(builder.influxdbVersion, is(InfluxDBCompatibilityVersions.LATEST));
        assertThat(builder.transformer, is(MetricMeasurementTransformer.NOOP));
    }
    
    @Test
    public void builder_api_with_compatibility_v08() {
        Influxdb influxdbMock = Mockito.mock(Influxdb.class);
        ScheduledReporter reporter = 
                InfluxdbReporter
                    .forRegistry(registry)
                    .v08(influxdbMock)
                    .build();
        
        assertThat(reporter, notNullValue());
    }

    @Test
    public void builder_api_with_protocol() {
        ScheduledReporter reporter = 
                InfluxdbReporter
                    .forRegistry(registry)
                    .protocol(InfluxdbProtocols.http())
                    .build();
        
        assertThat(reporter, notNullValue());
    }
    
    @Test
    public void builder_api_with_tranformer() {
        MetricMeasurementTransformer mmt = new MetricMeasurementTransformer() {
            @Override
            public Map<String, String> tags(String metricName) {
                return null;
            }
            
            @Override
            public String measurementName(String metricName) {
                return null;
            }
        };
        
        Builder builder = 
                InfluxdbReporter
                .forRegistry(registry)
                .transformer(mmt);
        
        assertThat(builder.transformer, notNullValue());
        assertThat(builder.transformer, is(mmt));
    }
    
    @Test
    public void builder_api_with_tags() {
    	String tagKey = "tag-name";
		String tagValue = "tag-value";
		
		Builder builder = InfluxdbReporter
		.forRegistry(registry)
		.tag(tagKey, tagValue)
		.protocol(InfluxdbProtocols.http());
    	
    	assertThat(builder.tags, notNullValue());
    	assertThat(builder.tags, hasEntry(tagKey, tagValue));
    	
		ScheduledReporter reporter = builder.build();
    	assertThat(reporter, notNullValue());
    }
    
    @Test(expected=NullPointerException.class)
    public void builder_api_with_tags_checksNullKey() {
        String tagValue = "tag-value";
        
        Builder builder = InfluxdbReporter
                .forRegistry(registry)
                .tag(null, tagValue);
    }
    
    @Test(expected=NullPointerException.class)
    public void builder_api_with_tags_checksNullValue() {
        String tagKey = "tag-name";
        
        Builder builder = InfluxdbReporter
                .forRegistry(registry)
                .tag(tagKey, null);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void builder_api_with_tags_checksEmptyKey() {
        String tagValue = "tag-value";
        
        Builder builder = InfluxdbReporter
                .forRegistry(registry)
                .tag("", tagValue);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void builder_api_with_tags_checksEmptyValue() {
        String tagKey = "tag-name";
        
        Builder builder = InfluxdbReporter
                .forRegistry(registry)
                .tag(tagKey, "");
    }
}
