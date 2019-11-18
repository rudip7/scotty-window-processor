package Benchmark.Old;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class OLDParallelThroughputStatistics {

	private static OLDParallelThroughputStatistics statistics;
	private boolean pause;

	private OLDParallelThroughputStatistics() {
	}

	private ArrayList<Double> counter = new ArrayList<>();
	private ArrayList<Double> sum = new ArrayList<>();
	private int parallelIndex = 0;

	public static Tuple2<Integer, OLDParallelThroughputStatistics> getInstance() {
		if (statistics == null)
			statistics = new OLDParallelThroughputStatistics();
		Tuple2<Integer, OLDParallelThroughputStatistics> actual = new Tuple2<>();
		actual.f0 = new Integer(statistics.parallelIndex);
		actual.f1 = statistics;
		statistics.counter.add((double) 0);
		statistics.sum.add((double) 0);
		statistics.parallelIndex++;

		return actual;
	}


	public void addThrouputResult(double throuputPerS, int index) {
		if (this.pause)
			return;
		counter.set(index, counter.get(index)+1);
		sum.set(index, sum.get(index)+throuputPerS);
	}

	public void clean() {
		counter = new ArrayList<>();
		sum = new ArrayList<>();
	}

	public double mean() {
		double totalSum = 0;
		double totalCounts = 0;
		for (int i = 0; i < counter.size(); i++) {
			totalSum += sum.get(i);
			totalCounts += counter.get(i);
		}
		return totalSum / totalCounts;
	}

	@Override
	public String toString() {
		return "\nThroughput Mean: " + mean();
	}

	public void pause(final boolean pause) {
		this.pause = pause;
	}
}
