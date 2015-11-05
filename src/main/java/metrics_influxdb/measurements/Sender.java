package metrics_influxdb.measurements;

import java.io.Closeable;
import java.util.Collection;

public interface Sender extends Closeable {
    /**
     * Flushes measurements still held and forces them to be sent.
     */
    public void flush();
    /**
     * Send the given {@link Measurement}.
     * Depending on the implementation, the {@link Sender} is allowed to enqueue the real sending action. 
     * @param m the measurement to be sent, if null this method is a NOOP
     */
    public void send(Measurement m);
    /**
     * Send the given measurements.
     * Depending on the implementation, the {@link Sender} is allowed to enqueue the real sending action. 
     * @param measures the measurements to be sent, if null this method is a NOOP
     */
    public void send(Collection<Measurement> measures);
}
