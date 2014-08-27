The library provide :

* a lighter client than influxdb-java to push only series to an [InfluxDB](http://influxdb.org) server.
* A reporter for [metrics](http://metrics.codahale.com/) which announces measurements.

The library provide a lighter client than influxdb-java to push only metrics.

## Dependencies :

* slf4j-api for logging.
* metrics-core, to provide if you use InfluxdbReporter.

## Usage sample :

	private static InfluxdbReporter startInfluxdbReporter(MetricRegistry registry) throws Exception {
		final Influxdb influxdb = new Influxdb("127.0.0.1", 8086, "mydb", "user", "pass");
		//influxdb.debugJson = true; // to print json on System.err
		//influxdb.jsonBuilder = new MyJsonBuildler(); // to use MyJsonBuilder to create json
		final InfluxdbReporter reporter = InfluxdbReporter
				.forRegistry(registry)
				.prefixedWith("test")
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS)
				.filter(MetricFilter.ALL)
				.build(influxdb);
		reporter.start(10, TimeUnit.SECONDS);
		return reporter;
	}

<p xmlns:dct="http://purl.org/dc/terms/">
  <a rel="license"
     href="http://creativecommons.org/publicdomain/zero/1.0/">
    <img src="http://i.creativecommons.org/p/zero/1.0/88x31.png" style="border-style: none;" alt="CC0" />
  </a>
  <br />
  To the extent possible under law,
  <a rel="dct:publisher"
     href="https://github.com/orgs/novaquark">
    <span property="dct:title">Novaquark</span></a>
  has waived all copyright and related or neighboring rights to
  this work.
</p>
