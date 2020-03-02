package StreamApprox;

/**
 * Any Configuration should be run as Stream Approx and our version of ReservoirSampling with Scotty and normal Flink
 */
public class ApproxConfiguration {

    public long runtime; // supposed runtime in ms
    public int throughput; // intended tuples/s generated by source
    public int sampleSize; // sample size per parallelism
    public int parallelism;
    public int iterations; // amount of times the job should run
    public int stratification; // amount of stratums the source should be split - 1 == no stratification
    public Environment environment; // StreamApprox, Scotty or Flink

}
