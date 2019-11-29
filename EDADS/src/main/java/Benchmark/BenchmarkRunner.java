package Benchmark;

import Synopsis.Histograms.EquiWidthHistogram;
import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.BiasedReservoirSampler;
import Synopsis.Sampling.ReservoirSampler;
import Synopsis.Sketches.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.tub.dima.scotty.core.TimeMeasure;
import de.tub.dima.scotty.core.windowType.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * Created by philipp on 5/28/17.
 */
public class BenchmarkRunner {

    private static String configPath;
    private static String outputPath;

    public static void main(String[] args) throws Exception {

        configPath = args[0];
        System.out.println("Configurations: "+configPath);

        outputPath = args[1];
        System.out.println("Output: "+outputPath);
//        configPath = "EDADS/src/main/java/Benchmark/Configurations/benchmark_CountMinSketch.json";
//        configPath = "EDADS/src/main/java/Benchmark/Configurations/benchmark_ReservoirSampler.json";

        BenchmarkConfig config = loadConfig();

        PrintWriter resultWriter = new PrintWriter(new FileOutputStream(new File(outputPath+"/result_" + config.name + ".txt"), true));

        Configuration conf = new Configuration();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Long, Long>> gaps = Collections.emptyList();
        if (config.sessionConfig != null)
            gaps = generateSessionGaps(config.sessionConfig.gapCount, (int) config.runtime, config.sessionConfig.minGapTime, config.sessionConfig.maxGapTime);
//            gaps = generateSessionGaps(config.sessionConfig.gapCount, (int) TimeMeasure.minutes(2).toMilliseconds(), config.sessionConfig.minGapTime, config.sessionConfig.maxGapTime);

        System.out.println(gaps);
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

        for (String envConf:
             config.configurations) {
            if (envConf.equals("Scotty")){
                for (List<String> windows : config.windowConfigurations) {
                    for (String syn : config.synopses) {
                        System.out.println("\n\n\n\n\n\n\n");

                        System.out.println("Start Benchmark with windows: " + config.windowConfigurations);
                        System.out.println("Desired throughput: " + config.throughput);
                        System.out.println("\n\n\n\n\n\n\n");
                        Tuple2<Class<? extends MergeableSynopsis>, Object[]> synopsis = getSynopsis(syn);
                        new ScottyBenchmarkJob(getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, synopsis.f1);

                        System.out.println(ParallelThroughputStatistics.getInstance().toString());


                        resultWriter.append(windows + " \t" + syn + " \t" +
                                ParallelThroughputStatistics.getInstance().mean() + "\t");
                        resultWriter.append("\n");
                        resultWriter.flush();
                        ParallelThroughputStatistics.getInstance().clean();

                        Thread.sleep(seconds(10).toMilliseconds());
                    }
                }

            } else if(envConf.equals("Flink")){
                //TODO
                for (List<String> windows : config.windowConfigurations) {
                    for (String syn : config.synopses) {
                        System.out.println("\n\n\n\n\n\n\n");

                        System.out.println("Start Benchmark with windows " + config.windowConfigurations);
                        System.out.println("\n\n\n\n\n\n\n");
                        Tuple2<Class<? extends MergeableSynopsis>, Object[]> synopsis = getSynopsis(syn);
                        new FlinkBenchmarkJob(getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, synopsis.f1);

                        System.out.println(ParallelThroughputStatistics.getInstance().toString());


                        resultWriter.append(windows + " \t" + syn + " \t" +
                                ParallelThroughputStatistics.getInstance().mean() + "\t");
                        resultWriter.append("\n");
                        resultWriter.flush();
                        ParallelThroughputStatistics.getInstance().clean();

                        Thread.sleep(seconds(10).toMilliseconds());
                    }
                }
            }
        }


        resultWriter.close();
    }

    private static Tuple2<Class<? extends MergeableSynopsis>, Object[]> getSynopsis(String syn){
        if (syn.equals("CountMinSketch")){
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(CountMinSketch.class, new Object[]{65536,5,7L});
        }else if (syn.equals("ReservoirSampler")){
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(ReservoirSampler.class, new Object[]{10000});
        }else if (syn.equals("BiasedReservoirSampler")){
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(BiasedReservoirSampler.class, new Object[]{500});
        }else if (syn.equals("EquiWidthHistogram")){
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(EquiWidthHistogram.class, new Object[]{0,100,8});
        }else if (syn.equals("BloomFilter")){
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(BloomFilter.class, new Object[]{10000000,80000,7L});
        }else if (syn.equals("CuckooFilter")){
            return null;
        }else if (syn.equals("FastAGMS")){
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(FastAGMS.class, new Object[]{4000,2000,7L});
        }else if (syn.equals("HyperLogLogSketch")){
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(HyperLogLogSketch.class, new Object[]{11,7L});
        }
        throw new IllegalArgumentException(syn+" is not a valid synopsis for benchmarking");
    }

    private static List<Window> getAssigners(List<String> config) {

        List<Window> multiQueryAssigner = new ArrayList<Window>();
        for (String windowConfig : config) {
            multiQueryAssigner.addAll(getAssigner(windowConfig));
        }
        System.out.println("Generated Windows: " + multiQueryAssigner.size() + " " + multiQueryAssigner);
        return multiQueryAssigner;
    }

    private static Collection<Window> getAssigner(String windowConfig) {

        // [Sliding(1,2), Tumbling(1), Session(2)]
        Pattern pattern = Pattern.compile("\\d+");
        if (windowConfig.startsWith("Sliding")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int WINDOW_SIZE = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_SLIDE = Integer.valueOf(matcher.group(0));
            return Collections.singleton(new SlidingWindow(WindowMeasure.Time, TimeMeasure.of(WINDOW_SIZE, TimeUnit.MILLISECONDS).toMilliseconds(), TimeMeasure.of(WINDOW_SLIDE, TimeUnit.MILLISECONDS).toMilliseconds()));
        }

        if (windowConfig.startsWith("Tumbling")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int WINDOW_SIZE = Integer.valueOf(matcher.group(0));
            return Collections.singleton(new TumblingWindow(WindowMeasure.Time, TimeMeasure.of(WINDOW_SIZE, TimeUnit.SECONDS).toMilliseconds()));
        }

        if (windowConfig.startsWith("Session")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int GAP_SIZE = Integer.valueOf(matcher.group(0));
            return Collections.singleton(new SessionWindow(WindowMeasure.Time, TimeMeasure.seconds(GAP_SIZE).toMilliseconds()));
        }

        if (windowConfig.startsWith("RandomSession")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int Session_NR = Integer.valueOf(matcher.group(0));
            matcher.find();
            int MinGap = Integer.valueOf(matcher.group(0));
            matcher.find();
            int MaxGap = Integer.valueOf(matcher.group(0));
            Random random = new Random(10);
            Collection<Window> resultList = new ArrayList<>();
            for (int i = 0; i < Session_NR; i++) {
                long GapSize = MinGap + random.nextInt(MaxGap - MinGap);
                resultList.add(new SessionWindow(WindowMeasure.Time, TimeMeasure.seconds(GapSize).toMilliseconds()));
            }
            return resultList;
        }
        if (windowConfig.startsWith("randomTumbling")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int WINDOW_NR = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_MIN_SIZE = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_MAX_SIZE = Integer.valueOf(matcher.group(0));
            Random random = new Random(10);
            Collection<Window> resultList = new ArrayList<>();
            for (int i = 0; i < WINDOW_NR; i++) {
                double WINDOW_SIZE = (WINDOW_MIN_SIZE + random.nextDouble() * (WINDOW_MAX_SIZE - WINDOW_MIN_SIZE));
                resultList.add(new TumblingWindow(WindowMeasure.Time, TimeMeasure.of((long) (WINDOW_SIZE * 1000), TimeUnit.MILLISECONDS).toMilliseconds()));
            }
            return resultList;
        }

        if (windowConfig.startsWith("randomCount")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int WINDOW_NR = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_MIN_SIZE = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_MAX_SIZE = Integer.valueOf(matcher.group(0));
            Random random = new Random(10);
            Collection<Window> resultList = new ArrayList<>();
            for (int i = 0; i < WINDOW_NR; i++) {
                double WINDOW_SIZE = WINDOW_MIN_SIZE + random.nextDouble() * (WINDOW_MAX_SIZE - WINDOW_MIN_SIZE);
                //int WINDOW_SIZE = random.nextInt(WINDOW_MAX_SIZE - WINDOW_MIN_SIZE) + WINDOW_MIN_SIZE;

                resultList.add(new TumblingWindow(WindowMeasure.Count, (int) WINDOW_SIZE));
            }
            return resultList;
        }
        return null;
    }

    public static List<Tuple2<Long, Long>> generateSessionGaps(int gapCount, int maxTs, int minGapTime, int maxGapTime) {
        Random gapStartRandom = new Random(333);
        Random gapLengthRandom = new Random(777);

        List<Tuple2<Long, Long>> result = new ArrayList<>();
        for (int i = 0; i < gapCount; i++) {
            long gapStart = gapStartRandom.nextInt(maxTs);
            long gapLength = gapLengthRandom.nextInt(maxGapTime - minGapTime) + minGapTime;
            result.add(new Tuple2<>(gapStart, gapLength));
        }

        result.sort(new Comparator<Tuple2<Long, Long>>() {
            @Override
            public int compare(final Tuple2<Long, Long> o1, final Tuple2<Long, Long> o2) {
                return Long.compare(o1.f0, o2.f0);
            }
        });
        return result;
    }


    private static BenchmarkConfig loadConfig() throws Exception {
        try (Reader reader = new InputStreamReader(new FileInputStream(configPath), "UTF-8")) {
            Gson gson = new GsonBuilder().create();
            return gson.fromJson(reader, BenchmarkConfig.class);
        }
    }

}
