package FlinkScottyConnector;

import Synopsis.Synopsis;
import org.apache.flink.streaming.api.windowing.time.Time;


public class BuildSynopsisConfig <S extends Synopsis> {

    Class<S> synopsisClass;
    int keyField = -1;
    Object[] synParams;

    public BuildSynopsisConfig(Class<S> synopsisClass, Object[] synParams) {
        this.synopsisClass = synopsisClass;
        this.synParams = synParams;
    }

    public BuildSynopsisConfig(int keyField, Class<S> synopsisClass, Object... params) {
        this.keyField = keyField;
        this.synopsisClass = synopsisClass;
        this.synParams = params;
    }

    public Class<S> getSynopsisClass() {
        return synopsisClass;
    }

    public void setSynopsisClass(Class<S> synopsisClass) {
        this.synopsisClass = synopsisClass;
    }


    public int getKeyField() {
        return keyField;
    }

    public Object[] getSynParams() {
        return synParams;
    }

    public void setSynParams(Object[] synParams) {
        this.synParams = synParams;
    }

    public void setKeyField(int keyField) {
        this.keyField = keyField;
    }

}
