package StreamApprox;


import Benchmark.FlinkBenchmarkJobs.NormalFlinkJob;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

public class Runner {

    private static String configPath;

    public static void main(String[] args) throws Exception{

        configPath = args[0];
        System.out.println("\n\nLoading configurations: "+configPath);

        BenchmarkList benchmark = loadConfig();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(env.getParallelism());

        for (int i = 0; i < benchmark.approxConfigurationList.size(); i++) {
            ApproxConfiguration config = benchmark.approxConfigurationList.get(i);

            if (config.parallelism <= 0 || config.parallelism > env.getMaxParallelism()) {
                throw new IllegalArgumentException("Illegal argument in configuration "+i+": Parallelism must be at least 1 and be less than "+env.getMaxParallelism()+" and was "+config.parallelism);
            }
            if (config.iterations <= 0){
                throw new IllegalArgumentException("Illegal argument in configuration "+i+": Number of iterations must be a positive number and was "+config.iterations);
            }
            env.setParallelism(config.parallelism);

            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

            if(config.environment == Environment.StreamApprox){
                for (int j = 0; j < config.iterations; j++) {
                    System.out.println("Iteration:" + j);
                    String configString = "StreamApprox:\t Parallelism: " + config.parallelism + " \t  Runtime: "+ config.runtime +" \t Throughput: "+ config.throughput +" \t Stratification: " + config.stratification;
                    System.out.println("Starting Benchmark:");
                    System.out.println(configString);
                    new StreamApproxJob(config, env, configString);
                }
            }
            if(config.environment == Environment.Flink){
                for (int j = 0; j < config.iterations; j++) {
                    System.out.println("Iteration:" + j);
                    String configString = "Flink ReservoirSampler:\t Parallelism: " + config.parallelism + " \t  Runtime: "+ config.runtime +" \t Throughput: "+ config.throughput +" \t Stratification: " + config.stratification;
                    System.out.println("Starting Benchmark:");
                    System.out.println(configString);

                    new NormalFlinkJob<Synopsis.Sampling.ReservoirSampler>(configString);
                }
                
                public NormalFlinkJob(String configuration, List< Window > assigners, StreamExecutionEnvironment env, final long runtime,
                final int throughput, final List<Tuple2<Long, Long>> gaps, Class<S> synopsisClass, boolean stratified, Object[] parameters) {



            }
            if(config.environment == Environment.Scotty){

            }
        }
    }


    private static BenchmarkList loadConfig() throws Exception{
        try (Reader reader = new InputStreamReader(new FileInputStream(configPath), "UTF-8")){
            Gson gson = new GsonBuilder().create();
            return gson.fromJson(reader, BenchmarkList.class);
        }
    }
}
