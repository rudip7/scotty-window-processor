package StreamApprox;


import Benchmark.FlinkBenchmarkJobs.NormalFlinkJob;
import Synopsis.Sampling.ReservoirSampler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

public class Runner {

    private static String configPath;

    public static void main(String[] args) throws Exception{

//        configPath = args[0];
        configPath = "C:\\Users\\Rudi\\Documents\\EDADS\\scotty-window-processor\\EDADS\\src\\main\\java\\StreamApprox\\StreamApproxConfigs";
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

            String configString = config.environment + ":\t Parallelism: " + config.parallelism + " \t  Runtime: "+ config.runtime +" \t Throughput: "+ config.throughput +" \t Stratification: " + config.stratification;

            if(config.environment == Environment.StreamApprox){
                for (int j = 0; j < config.iterations; j++) {
                    System.out.println("Iteration:" + j);
                    System.out.println("Starting Benchmark:");
                    System.out.println(configString);
                    new StreamApproxJob(config, env, configString);
                }
            }
            if(config.environment == Environment.Flink){
                for (int j = 0; j < config.iterations; j++) {
                    System.out.println("Iteration:" + j);
                    System.out.println("Starting Benchmark:");
                    System.out.println(configString);

                    new FlinkBenchmarkJob(config, env,configString);
                }
            }
            if(config.environment == Environment.Scotty){
                for (int j = 0; j < config.iterations; j++) {
                    System.out.println("Iteration:" + j);
                    System.out.println("Starting Benchmark:");
                    System.out.println(configString);

                    new ScottyBenchmarkJob(config, env,configString);
                }
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
