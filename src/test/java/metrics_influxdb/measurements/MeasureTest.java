package metrics_influxdb.measurements;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class MeasureTest {

		@Test
		public void nanOrInfiniteValueSkippedTest() {

				Measure m = new Measure("name");
				m.addValue("one", 1.0);
				assertTrue(m.getValues().size() == 1);

				m.addValue("Float.NaN", Float.NaN);
				assertTrue(m.getValues().size() == 1);

				m.addValue("Float.NEGATIVE_INFINITY", Float.NEGATIVE_INFINITY);
				assertTrue(m.getValues().size() == 1);

				m.addValue("Float.POSITIVE_INFINITY", Float.POSITIVE_INFINITY);
				assertTrue(m.getValues().size() == 1);

				m.addValue("Double.NaN", Double.NaN);
				assertTrue(m.getValues().size() == 1);

				m.addValue("Double.NEGATIVE_INFINITY", Double.NEGATIVE_INFINITY);
				assertTrue(m.getValues().size() == 1);

				m.addValue("Double.POSITIVE_INFINITY", Double.POSITIVE_INFINITY);
				assertTrue(m.getValues().size() == 1);

				m.addValue("two", 2);
				assertTrue(m.getValues().size() == 2);

		}
}
