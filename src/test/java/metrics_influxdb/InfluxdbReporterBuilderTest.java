package metrics_influxdb;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
        String tag = "tag1";
        String tagValue = "val1";

        Influxdb influxdbMock = Mockito.mock(Influxdb.class);
        ScheduledReporter reporter = 
                InfluxdbReporter
                    .forRegistry(registry)
                    .v08(influxdbMock)
                    .tag(tag, tagValue)
                    .build();

        assertThat(reporter, notNullValue());
        assertTrue(reporter instanceof InfluxdbReporter);

        InfluxdbReporter influxReporter = (InfluxdbReporter) reporter;

        // verify tags are added to the column arrays
        assertEquals(influxReporter.timerColumns[influxReporter.timerColumns.length - 1], tag);
        assertEquals(influxReporter.histogramColumns[influxReporter.histogramColumns.length - 1], tag);
        assertEquals(influxReporter.countColumns[influxReporter.countColumns.length - 1], tag);
        assertEquals(influxReporter.gaugeColumns[influxReporter.gaugeColumns.length - 1], tag);
        assertEquals(influxReporter.meterColumns[influxReporter.meterColumns.length - 1], tag);

        // verify tag values are added to the point arrays
        assertEquals(influxReporter.timerPoints[0][influxReporter.timerPoints[0].length - 1], tagValue);
        assertEquals(influxReporter.histogramPoints[0][influxReporter.histogramPoints[0].length - 1], tagValue);
        assertEquals(influxReporter.countPoints[0][influxReporter.countPoints[0].length - 1], tagValue);
        assertEquals(influxReporter.gaugePoints[0][influxReporter.gaugePoints[0].length - 1], tagValue);
        assertEquals(influxReporter.meterPoints[0][influxReporter.meterPoints[0].length - 1], tagValue);

        // verify that the arrays are still the same size
        assertEquals(influxReporter.timerColumns.length, influxReporter.timerPoints[0].length);
        assertEquals(influxReporter.histogramColumns.length, influxReporter.histogramPoints[0].length);
        assertEquals(influxReporter.countColumns.length, influxReporter.countPoints[0].length);
        assertEquals(influxReporter.gaugeColumns.length, influxReporter.gaugePoints[0].length);
        assertEquals(influxReporter.meterColumns.length, influxReporter.meterPoints[0].length);
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
