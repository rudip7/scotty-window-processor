package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BuildSynopsisConfig {
    Time windowTime;
    Time slideTime;
    int keyField = -1;


    /**
     *
     * @param windowTime    Window Time
     * @param slideTime     Slide Time
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     */
    public BuildSynopsisConfig(Time windowTime, Time slideTime, int keyField) {
        this.windowTime = windowTime;
        this.slideTime = slideTime;
        this.keyField = keyField;
    }

    public Time getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(Time windowTime) {
        this.windowTime = windowTime;
    }

    public Time getSlideTime() {
        return slideTime;
    }

    public void setSlideTime(Time slideTime) {
        this.slideTime = slideTime;
    }

    public int getKeyField() {
        return keyField;
    }

    public void setKeyField(int keyField) {
        this.keyField = keyField;
    }

}
