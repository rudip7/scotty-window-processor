package Benchmark;

import Benchmark.FlinkBenchmarkJobs.*;
import Benchmark.ScottyBenchmarkJobs.*;
import Synopsis.Histograms.EquiWidthHistogram;
import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.BiasedReservoirSampler;
import Synopsis.Sampling.ReservoirSampler;
import Synopsis.Sketches.*;
import Synopsis.Synopsis;
import Synopsis.Wavelets.WaveletSynopsis;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.tub.dima.scotty.core.TimeMeasure;
import de.tub.dima.scotty.core.windowType.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BenchmarkRunner {

    private static String configPath;

    public static void main(String[] args) throws Exception {

        configPath = args[0];
        System.out.println("\n\nLoading configurations: " + configPath);


//        configPath = "EDADS/src/main/java/Benchmark/Configurations/waveletConfig.json";

        Benchmark benchmark = loadConfig();


//        BenchmarkConfig config = loadConfig2();

//        Configuration conf = new Configuration();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int maxParallelism = env.getParallelism();

        for (int k = 0; k < benchmark.benchmarkConfigurations.size(); k++) {
            BenchmarkConfig config = benchmark.benchmarkConfigurations.get(k);
            if (config.iterations < 0) {
                throw new IllegalArgumentException("Illegal argument in configuration " + k + ": Number of iterations must be a positive number and was " + config.iterations);
            } else if (config.iterations == 0) {
                config.iterations = 1;
            }
            if (config.parallelism == 0) {
                env.setParallelism(maxParallelism);
                env.setMaxParallelism(maxParallelism);
            } else {
                env.setParallelism(config.parallelism);
                env.setMaxParallelism(config.parallelism);
            }

            List<Tuple2<Long, Long>> gaps = Collections.emptyList();
            if (config.sessionConfig != null)
                gaps = generateSessionGaps(config.sessionConfig.gapCount, (int) config.runtime, config.sessionConfig.minGapTime, config.sessionConfig.maxGapTime);
//            gaps = generateSessionGaps(config.sessionConfig.gapCount, (int) TimeMeasure.minutes(2).toMilliseconds(), config.sessionConfig.minGapTime, config.sessionConfig.maxGapTime);

            System.out.println(gaps);
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);


            for (String envConf :
                    config.configurations) {
                if (envConf.equals("Scotty")) {
                    for (List<String> windows : config.windowConfigurations) {
                        for (String syn : config.synopses) {
                            System.out.println("\n\n");
                            for (int i = 0; i < config.iterations; i++) {
                                System.out.println("Iteration: " + i);
                                String configuration = "Scotty:\t Parallelism: " + env.getParallelism() + " \t" + config.source + " \t" + windows + " \t" + config.throughput + " \t";
                                if (config.stratified) {
                                    configuration += "Stratified " + syn + " \t";
                                } else {
                                    configuration += syn + " \t";
                                }
                                System.out.println("Starting Benchmark:");
                                System.out.println(configuration);
                                System.out.println("Desired throughput: " + config.throughput);
                                System.out.println("\n\n");
                                if (syn.equals("Wavelet")) {
                                    new WaveletScottyJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, config.source, config.stratified);
                                } else {
                                    Tuple2<Class<? extends MergeableSynopsis>, Object[]> synopsis = getSynopsis(syn);
                                    if (config.source.contentEquals("Normal")) {
                                        new NormalScottyJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, config.stratified, synopsis.f1);
                                    } else if (config.source.contentEquals("Zipf")) {
                                        new ZipfScottyJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, config.stratified, synopsis.f1);
                                    } else if (config.source.contentEquals("Uniform")) {
                                        new UniformScottyJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, config.stratified, synopsis.f1);
                                    } else if (config.source.contentEquals("NYC-taxi")) {
                                        new NYCScottyJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, config.stratified, synopsis.f1);
                                    } else {
                                        throw new IllegalArgumentException("Source not supported: " + config.source + " ; Available sources are: Normal, Zipf, Uniform, NYC-taxi");
                                    }
                                }
                            }
                        }
                    }

                } else if (envConf.equals("Flink")) {
                    for (List<String> windows : config.windowConfigurations) {
                        for (String syn : config.synopses) {
                            System.out.println("\n\n");
                            for (int i = 0; i < config.iterations; i++) {
                                System.out.println("Iteration: " + i);
                                String configuration = "Flink:\t Parallelism: " + env.getParallelism() + " \t" + config.source + " \t" + windows + " \t" + config.throughput + " \t";
                                if (config.stratified) {
                                    configuration += "Stratified " + syn + " \t";
                                } else {
                                    configuration += syn + " \t";
                                }
                                System.out.println("Starting Benchmark:");
                                System.out.println(configuration);
                                System.out.println("\n\n");
                                if (syn.equals("Wavelet")) {
                                    new WaveletFlinkJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, config.source, config.stratified);
                                } else {
                                    Tuple2<Class<? extends MergeableSynopsis>, Object[]> synopsis = getSynopsis(syn);
                                    if (config.source.contentEquals("Normal")) {
                                        new NormalFlinkJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, config.stratified, synopsis.f1);
                                    } else if (config.source.contentEquals("Zipf")) {
                                        new ZipfFlinkJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, config.stratified, synopsis.f1);
                                    } else if (config.source.contentEquals("Uniform")) {
                                        new UniformFlinkJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, config.stratified, synopsis.f1);
                                    } else if (config.source.contentEquals("NYC-taxi")) {
                                        new NYCFlinkJob(configuration, getAssigners(windows), env, config.runtime, config.throughput, gaps, synopsis.f0, config.stratified, synopsis.f1);
                                    } else {
                                        throw new IllegalArgumentException("Source not supported: " + config.source + " ; Available sources are: Normal, Zipf, Uniform, NYC-taxi");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static Tuple2<Class<? extends MergeableSynopsis>, Object[]> getSynopsis(String syn) {
        if (syn.equals("CountMinSketch")) {
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(CountMinSketch.class, new Object[]{65536, 5, 7L});
//            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(CountMinSketch.class, new Object[]{10, 5, 7L});
        } else if (syn.equals("ReservoirSampler")) {
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(ReservoirSampler.class, new Object[]{10000});
//            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(ReservoirSampler.class, new Object[]{10});
        } else if (syn.equals("BiasedReservoirSampler")) {
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(BiasedReservoirSampler.class, new Object[]{500});
        } else if (syn.equals("EquiWidthHistogram")) {
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(EquiWidthHistogram.class, new Object[]{0.0, 101.0, 10});
        } else if (syn.equals("BloomFilter")) {
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(BloomFilter.class, new Object[]{10000000, 80000, 7L});
        } else if (syn.equals("CuckooFilter")) {
            return null;
        } else if (syn.equals("FastAGMS")) {
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(FastAGMS.class, new Object[]{4000, 2000, 7L});
        } else if (syn.equals("HyperLogLogSketch")) {
            return new Tuple2<Class<? extends MergeableSynopsis>, Object[]>(HyperLogLogSketch.class, new Object[]{11, 7L});
        }
        throw new IllegalArgumentException(syn + " is not a valid synopsis for benchmarking");
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


    private static Benchmark loadConfig() throws Exception {
        try (Reader reader = new InputStreamReader(new FileInputStream(configPath), "UTF-8")) {
            Gson gson = new GsonBuilder().create();
            return gson.fromJson(reader, Benchmark.class);
        }
    }

}
