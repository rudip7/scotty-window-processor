package ApproximateDataAnalytics;

import java.io.Serializable;

public class StratifiedTimestampedQuery<P extends Serializable> extends TimestampedQuery{
    private P partition;

    public StratifiedTimestampedQuery(Serializable query, long timeStamp, P partition) {
        super(query, timeStamp);
        this.partition = partition;
    }

    public P getPartition() {
        return partition;
    }
}
