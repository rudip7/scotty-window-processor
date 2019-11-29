package Benchmark;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ParallelThroughputLogger<T> extends RichFlatMapFunction<T, T> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelThroughputLogger.class);

    private long totalReceived;
    private long lastTotalReceived;
    private long lastLogTimeMs;
    //	private int elementSize;
    private long logfreq;
    private boolean parallelismSet = false;
//    private PrintWriter resultWriter;

    public ParallelThroughputLogger(long logfreq) {
//		this.elementSize = elementSize;
        this.logfreq = logfreq;
        this.totalReceived = 0;
        this.lastTotalReceived = 0;
        this.lastLogTimeMs = -1;
//        resultWriter = new PrintWriter(new FileOutputStream(new File(outputPath+"/result_" + config.name + ".txt"), true));

    }

    @Override
    public void close() throws Exception {
        LOG.info(ParallelThroughputStatistics.getInstance().toString());
//        System.out.println(ParallelThroughputStatistics.getInstance().toString());
    }

    @Override
    public void flatMap(T element, Collector<T> collector) throws Exception {
        collector.collect(element);
        if (!parallelismSet) {
            ParallelThroughputStatistics.setParallelism(this.getRuntimeContext().getNumberOfParallelSubtasks());
            parallelismSet = true;
        }
        totalReceived = totalReceived + 1;
        long now = System.currentTimeMillis();
        if (lastLogTimeMs == -1) {
            // init (the first)
            lastLogTimeMs = now;
        }
        long timeDiff = now - lastLogTimeMs;
        if (timeDiff > logfreq) {
            long elementDiff = totalReceived - lastTotalReceived;
            double ex = (1000 / (double) timeDiff);
            LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. ",
                    timeDiff, elementDiff, elementDiff * ex);

            ParallelThroughputStatistics.getInstance().addThrouputResult(elementDiff * ex);
            //System.out.println(ThroughputStatistics.getInstance().toString());
            // reinit
            lastLogTimeMs = now;
            lastTotalReceived = totalReceived;
        }
    }

}
