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

[![Build Status](https://travis-ci.org/davidB/metrics-influxdb.svg?branch=master)](https://travis-ci.org/davidB/metrics-influxdb)
[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/davidB/metrics-influxdb/trend.png)](https://bitdeli.com/free "Bitdeli Badge")
[![Download](https://api.bintray.com/packages/davidb/maven/metrics-influxdb/images/download.svg) ](https://bintray.com/davidb/maven/metrics-influxdb/_latestVersion)

# The library provide :

* a lighter client than influxdb-java to push only series to an [InfluxDB](http://influxdb.org) server.
* A reporter for [metrics](http://metrics.codahale.com/) which announces measurements.

The library provide a lighter client than influxdb-java to push only metrics.

## Dependencies :

* slf4j-api for logging.
* metrics-core, to provide, if you use InfluxdbReporter.

## Install:

### Released
```
 dependencies {
	compile 'com.github.davidb:metrics-influxdb:0.9.3'
 }
```

### Dev
```
 repositories {
    maven { url "https://jitpack.io" }
 }
 dependencies {
	compile 'com.github.davidb:metrics-influxdb:-SNAPSHOT'
 }
```
## Usage :

Using the Builder API and its defaults, it is easy to use InfluxdbReporter:

    ScheduledReporter reporter = InfluxdbReporter.forRegistry(registry).build();
    reporter.start(10, TimeUnit.SECONDS);

With the previous simple configuration, all defaults will be used, mainly:

- protocol: `HTTP`
- server: `127.0.0.1`
- port: `8086`
- authentication: `none`
- database name: `metrics`
- rates: converted to `TimeUnit.SECONDS`
- duration: converted to `TimeUnit.MILLISECONDS`
- idle metrics: `do not report`
- influxdb protocol: `v09` [line protocol](https://influxdb.com/docs/v0.9/write_protocols/line.html)
- ...

But you are free of course to define all settings by yourself :
```
final ScheduledReporter reporter = InfluxdbReporter.forRegistry(registry)
    .protocol(new HttpInfluxdbProtocol("http", "influxdb-server", 8086, "admin", "53CR3TP455W0RD", "metrics"))
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .filter(MetricFilter.ALL)
    .skipIdleMetrics(false)
    .tag("cluster", "CL01")
    .tag("client", "OurImportantClient")
    .tag("server", serverIP)
    .transformer(new CategoriesMetricMeasurementTransformer("module", "artifact"))
    .build();
reporter.start(10, TimeUnit.SECONDS);
```

And if you are still using v08 influxdb

```
final InfluxdbReporter reporter = InfluxdbReporter
    .forRegistry(registry)
    .protocol(new HttpInfluxdbProtocol("influxdb-server", 8086, "admin", "53CR3TP455W0RD", "metrics"))
    .v08()
    .build();
...
```

