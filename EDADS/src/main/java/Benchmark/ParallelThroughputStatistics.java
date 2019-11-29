package Benchmark;

public class ParallelThroughputStatistics {

	private static ParallelThroughputStatistics statistics;
	private boolean pause;
	private int parallelism = 1;

	private ParallelThroughputStatistics() {
	}

	private double counter = 0;
	private double sum = 0;
	public static ParallelThroughputStatistics getInstance() {
		if (statistics == null)
			statistics = new ParallelThroughputStatistics();
		return statistics;
	}

	public static void setParallelism(int parallelism) {
		if (statistics == null)
			statistics = new ParallelThroughputStatistics();
		statistics.parallelism = parallelism;
	}


	public void addThrouputResult(double throuputPerS) {
		if (this.pause)
			return;
		counter += ((double) 1)/parallelism;
		sum += throuputPerS;
	}

	public void clean() {
		counter = 0;
		sum = 0;
	}

	public double mean() {
		return sum / counter;
	}

	@Override
	public String toString() {
		return "Throughput Mean: " + mean();
	}

	public void pause(final boolean pause) {
		this.pause = pause;
	}
}
