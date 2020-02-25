package Benchmark;

import java.util.List;

public class BenchmarkConfig {

	public int throughput;
	public long runtime;
	public String name;
	public boolean stratified;
//	public int parallelism;
//	public int iterations;

	// [Sliding(1,2), Tumbling(1), Session(2)]
	public List<List<String>> windowConfigurations;

	// Scotty, Flink
	public List<String> configurations;

	// Normal, Zipf, Uniform, NYC-taxi
	public String source;

	// [CountMinSketch, BloomFilter, ReservoirSampling]
	public List<String> synopses;

	public SessionConfig sessionConfig;


	public class SessionConfig {
		int gapCount = 0;
		int minGapTime = 0;
		int maxGapTime = 0;
	}
}
