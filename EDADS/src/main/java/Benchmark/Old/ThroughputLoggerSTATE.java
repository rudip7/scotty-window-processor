package Benchmark.Old;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;

public class ThroughputLoggerSTATE<T> extends RichFlatMapFunction<T, Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(ThroughputLoggerSTATE.class);

	private LongState totalReceived;
	private LongState lastTotalReceived;
	private LongState lastLogTimeMs;
//	private int elementSize;
	private long logfreq;

	public ThroughputLoggerSTATE(long logfreq) {
//		this.elementSize = elementSize;
		this.logfreq = logfreq;
		this.totalReceived = new LongState(0);
		this.lastTotalReceived = new LongState(0);
		this.lastLogTimeMs = new LongState(-1);
	}

	@Override
	public void flatMap(T element, Collector<Integer> collector) throws Exception {
		totalReceived.update(totalReceived.value() + 1);
		if (totalReceived.value() % logfreq == 0) {
			// throughput over entire time
			long now = System.currentTimeMillis();

			// throughput for the last "logfreq" elements
			if (lastLogTimeMs.value() == -1) {
				// init (the first)
				lastLogTimeMs.update(now);
				lastTotalReceived.update(totalReceived.value);
			} else {
				long timeDiff = now - lastLogTimeMs.value();
				long elementDiff = totalReceived.value() - lastTotalReceived.value();
				double ex = (1000 / (double) timeDiff);
				LOG.error("During the last {} ms, we received {} elements. That's {} elements/second/core. ",
					timeDiff, elementDiff, elementDiff * ex);

				ThroughputStatistics.getInstance().addThrouputResult(elementDiff * ex);
				//Environment.out.println(ThroughputStatistics.getInstance().toString());
				// reinit
				lastLogTimeMs.update(now);
				lastTotalReceived.update(totalReceived.value);
			}
		}
	}

	public static class LongState implements ValueState<Long>, Serializable {

		long value;

		public LongState(long value) {
			this.value = value;
		}

		@Override
		public Long value() throws IOException {
			return value;
		}

		@Override
		public void update(Long value) throws IOException {
			this.value = value;
		}

		@Override
		public void clear() {
			value = 0;
		}

		private void writeObject(ObjectOutputStream out) throws IOException {
			out.writeLong(value);
		}

		private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
			value = in.readLong();
		}

		private void readObjectNoData() throws ObjectStreamException {
			System.out.println("readObjectNoData() called - should give an exception");
		}

	}
}
