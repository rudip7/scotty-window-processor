package ApproximateDataAnalytics;

import java.io.Serializable;

public class TimestampedQuery<Q extends Serializable> implements Serializable{
    private final Q query;
    private final long timeStamp;

    public TimestampedQuery(Q query, long timeStamp) {
        this.query = query;
        this.timeStamp = timeStamp;
    }

    public Q getQuery() {
        return query;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
