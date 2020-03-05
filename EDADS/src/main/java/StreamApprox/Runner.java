package StreamApprox;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
public class Runner {

    private static String configPath;

    public static void main(String[] args) throws Exception{



//        configPath = args[0];
        configPath = "/Users/joschavonhein/Workspace/scotty-window-processor/EDADS/src/main/java/StreamApprox/testLocal.json";
        System.out.println("\n\nLoading configurations: "+configPath);

        BenchmarkList benchmark = loadConfig();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(env.getParallelism());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        for (int i = 0; i < benchmark.approxConfigurationList.size(); i++) {

            ApproxConfiguration config = benchmark.approxConfigurationList.get(i);

            // set stratification as a global job parameter
            Configuration conf = new Configuration();
            conf.setInteger("stratification", config.stratification);
            env.getConfig().setGlobalJobParameters(conf);

            if (config.parallelism <= 0 || config.parallelism > env.getMaxParallelism()) {
                throw new IllegalArgumentException("Illegal argument in configuration "+i+": Parallelism must be at least 1 and be less than "+env.getMaxParallelism()+" and was "+config.parallelism);
            }
            if (config.iterations <= 0){
                throw new IllegalArgumentException("Illegal argument in configuration "+i+": Number of iterations must be a positive number and was "+config.iterations);
            }

            env.setParallelism(config.parallelism);
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            String configString = config.environment  + "\t Parallelism: " + config.parallelism
                    + " \t  Source: " + config.source + "\t Runtime: "+ config.runtime
                    + " \t Throughput: "+ config.throughput +" \t Stratification: " + config.stratification
                    + " \t Iterations: " + config.iterations;

            for (int j = 0; j < config.iterations; j++) {
                System.out.println("Iteration: " + j);
                System.out.println("Starting Benchmark:");
                System.out.println(configString);
                if(config.environment == Environment.StreamApprox){
                    new StreamApproxJob(config, env, configString);
                }
                if(config.environment == Environment.Flink){
                    new FlinkBenchmarkJob(config, env,configString);
                }
                if(config.environment == Environment.Scotty) {
                    new ScottyBenchmarkJob(config, env, configString);
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
