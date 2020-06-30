package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BuildSynopsisConfig {
    Time windowTime;
    Time slideTime;
    int keyField = -1;
    Object[] parameters;

    public BuildSynopsisConfig(Object[] parameters) {
        this.parameters = parameters;
    }

    public BuildSynopsisConfig(Time windowTime, Time slideTime, int keyField, Object[] parameters) {
        this.windowTime = windowTime;
        this.slideTime = slideTime;
        this.keyField = keyField;
        this.parameters = parameters;
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

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }
}
