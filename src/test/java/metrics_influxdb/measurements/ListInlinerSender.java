package metrics_influxdb.measurements;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import metrics_influxdb.serialization.line.Inliner;

public class ListInlinerSender extends QueueableSender {
    private Inliner inliner;
    private List<String> frames;

    public ListInlinerSender(int queueSize) {
        super(queueSize);
        inliner = new Inliner();
        frames = new LinkedList<>();
    }
    
    @Override
    protected boolean doSend(Collection<Measurement> measuresToSend) {
        return frames.add(inliner.inline(measuresToSend));
    }

    public List<String> getFrames() {
        return frames;
    }
}
