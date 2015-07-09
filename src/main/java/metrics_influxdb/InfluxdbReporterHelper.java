package metrics_influxdb;

import java.util.Arrays;
import java.util.Objects;

public class InfluxdbReporterHelper {
	public static final String TAG = "tag";
	// Optimization : use pointsXxx to reduce object creation, by reuse as arg of
	// Influxdb.appendSeries(...)
	private static final Object[] POINTS_TIMER = {
			0l,
			0,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0l
	};
	private static final Object[] POINTS_HISTOGRAM = {
			0l,
			0,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0.0d,
			0l
	};
	private static final Object[] POINTS_COUNTER = {
			0l,
			0l
	};
	private static final Object[] POINTS_GAUGE = {
			0l,
			null
	};
	private static final Object[] POINTS_METER = {
			0l,
			0,
			0.0d,
			0.0d,
			0.0d,
			0.0d
	};
	private static String[] COLUMNS_TIMER = {
			"time", "count"
			, "min", "max", "mean", "std-dev"
			, "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
			, "one-minute", "five-minute", "fifteen-minute", "mean-rate"
			, "run-count"
	};
	private static String[] COLUMNS_HISTOGRAM = {
			"time", "count"
			, "min", "max", "mean", "std-dev"
			, "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
			, "run-count"
	};
	private static String[] COLUMNS_COUNT = {
			"time", "count"
	};
	private static String[] COLUMNS_GAUGE = {
			"time", "value"
	};
	private static String[] COLUMNS_METER = {
			"time", "count"
			, "one-minute", "five-minute", "fifteen-minute", "mean-rate"
	};
	private final String[] columnsTimer;
	private final String[] columnsHistogram;
	private final String[] columnsCount;
	private final String[] columnsGauge;
	private final String[] columnsMeter;

	private final Object[][] pointsTimer;
	private final Object[][] pointsHistogram;
	private final Object[][] pointsCounter;
	private final Object[][] pointsGauge;
	private final Object[][] pointsMeter;


	public InfluxdbReporterHelper(String tag) {
		int tagIncrease = Objects.equals(tag, "") ? 0 : 1;

		columnsTimer = Arrays.copyOf(COLUMNS_TIMER, COLUMNS_TIMER.length + tagIncrease);
		columnsHistogram = Arrays.copyOf(COLUMNS_HISTOGRAM, COLUMNS_HISTOGRAM.length + tagIncrease);
		columnsCount = Arrays.copyOf(COLUMNS_COUNT, COLUMNS_COUNT.length + tagIncrease);
		columnsGauge = Arrays.copyOf(COLUMNS_GAUGE, COLUMNS_GAUGE.length + tagIncrease);
		columnsMeter = Arrays.copyOf(COLUMNS_METER, COLUMNS_METER.length + tagIncrease);

		pointsTimer = new Object[1][];
		pointsHistogram = new Object[1][];
		pointsCounter = new Object[1][];
		pointsGauge = new Object[1][];
		pointsMeter = new Object[1][];

		pointsTimer[0] = Arrays.copyOf(POINTS_TIMER, POINTS_TIMER.length + tagIncrease);
		pointsHistogram[0] = Arrays.copyOf(POINTS_HISTOGRAM, POINTS_HISTOGRAM.length + tagIncrease);
		pointsCounter[0] = Arrays.copyOf(POINTS_COUNTER, POINTS_COUNTER.length + tagIncrease);
		pointsGauge[0] = Arrays.copyOf(POINTS_GAUGE, POINTS_GAUGE.length + tagIncrease);
		pointsMeter[0] = Arrays.copyOf(POINTS_METER, POINTS_METER.length + tagIncrease);

		if (tagIncrease == 1) {
			columnsTimer[COLUMNS_TIMER.length] = TAG;
			columnsHistogram[COLUMNS_HISTOGRAM.length] = TAG;
			columnsCount[COLUMNS_COUNT.length] = TAG;
			columnsGauge[COLUMNS_GAUGE.length] = TAG;
			columnsMeter[COLUMNS_METER.length] = TAG;

			pointsTimer[0][POINTS_TIMER.length] = tag;
			pointsHistogram[0][POINTS_HISTOGRAM.length] = tag;
			pointsCounter[0][POINTS_COUNTER.length] = tag;
			pointsGauge[0][POINTS_GAUGE.length] = tag;
			pointsMeter[0][POINTS_METER.length] = tag;
		}
	}

	public String[] getColumnsTimer() {
		return columnsTimer;
	}

	public String[] getColumnsHistogram() {
		return columnsHistogram;
	}

	public String[] getColumnsCount() {
		return columnsCount;
	}

	public String[] getColumnsGauge() {
		return columnsGauge;
	}

	public String[] getColumnsMeter() {
		return columnsMeter;
	}

	public Object[][] getPointsTimer() {
		return pointsTimer;
	}

	public Object[][] getPointsHistogram() {
		return pointsHistogram;
	}

	public Object[][] getPointsCounter() {
		return pointsCounter;
	}

	public Object[][] getPointsGauge() {
		return pointsGauge;
	}

	public Object[][] getPointsMeter() {
		return pointsMeter;
	}
}
