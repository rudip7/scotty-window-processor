package Source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.XORShiftRandom;

import java.io.Serializable;
import java.util.Random;

public class DemoSource extends RichSourceFunction<Tuple3<Integer, Integer, Long>> implements Serializable {

    private Random key;
    private Random value;
    private boolean canceled = false;

    private int median = 10;
    private int standardDeviation = 3;

    private int seconds;


    /**
     * This parameter configures the watermark delay.
     */
    private long watermarkDelay = 1000;

    public DemoSource(){}

    public DemoSource(int seconds){this.seconds = seconds;}

    public DemoSource(long watermarkDelay){
        this.watermarkDelay = watermarkDelay;
    }

    public DemoSource(long watermarkDelay, int median, int standardDeviation){
        this.watermarkDelay = watermarkDelay;
        this.standardDeviation = standardDeviation;
        this.median = median;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.key = new XORShiftRandom(42);
        this.value = new XORShiftRandom(43);
    }

    public long lastWaterMarkTs = 0;

    @Override
    public void run(SourceContext<Tuple3<Integer, Integer, Long>> ctx) throws Exception {
        while (!canceled || seconds != 0) {
            int newKey = (int) (standardDeviation*key.nextGaussian() + median);
            if (newKey < 0){
                continue;
            }
            long timeStamp = System.currentTimeMillis();
            ctx.collectWithTimestamp(new Tuple3<>(newKey, value.nextInt(10), timeStamp), timeStamp);
            if (lastWaterMarkTs + 1000 < System.currentTimeMillis()) {
                long watermark = System.currentTimeMillis() - watermarkDelay;
                ctx.emitWatermark(new Watermark(watermark));
                lastWaterMarkTs = System.currentTimeMillis();
                seconds--;
                if (seconds == 0){
                    cancel();
                }

            }
            Thread.sleep(1);
        }
    }

    @Override
    public void cancel() {
        canceled = true;
    }
}