package metrics_influxdb.measurements;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import metrics_influxdb.serialization.line.Inliner;

public class ListInlinerSender extends QueueableSender {
	private Inliner inliner;
	private List<String> frames;

	public ListInlinerSender(int queueSize) {
		super(queueSize);
		inliner = new Inliner(TimeUnit.MILLISECONDS);
		frames = new LinkedList<>();
	}

	@Override
	protected boolean doSend(Collection<Measure> measuresToSend) {
		return frames.add(inliner.inline(measuresToSend));
	}

	public List<String> getFrames() {
		return frames;
	}
}
