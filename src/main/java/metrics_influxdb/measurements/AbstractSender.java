package metrics_influxdb.measurements;

import java.util.Collection;

public abstract class AbstractSender implements Sender {
	@Override
	public void send(Collection<Measure> measures) {
		for (Measure m : measures) {
			send(m);
		}
	}
}
