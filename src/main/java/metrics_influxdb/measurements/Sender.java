package metrics_influxdb.measurements;

import java.io.Closeable;
import java.util.Collection;

public interface Sender extends Closeable {
	/**
	 * Flushes measurements still held and forces them to be sent.
	 */
	public void flush();
	/**
	 * Send the given {@link Measure}.
	 * Depending on the implementation, the {@link Sender} is allowed to enqueue the real sending action. 
	 * @param m the Measure to be sent, if null this method is a NOOP
	 */
	public void send(Measure m);
	/**
	 * Send the given Measures.
	 * Depending on the implementation, the {@link Sender} is allowed to enqueue the real sending action. 
	 * @param measures the Measures to be sent, if null this method is a NOOP
	 */
	public void send(Collection<Measure> measures);
}
