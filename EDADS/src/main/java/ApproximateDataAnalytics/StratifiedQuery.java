package ApproximateDataAnalytics;

import org.apache.flink.api.java.functions.KeySelector;

import java.io.Serializable;

public class StratifiedQuery<Q extends Serializable, P extends Serializable> implements Serializable {
    private Q query;
    private P partition;

    public StratifiedQuery(Q query, P partition) {
        this.query = query;
        this.partition = partition;
    }

    public Q getQuery() {
        return query;
    }

    public P getPartition() {
        return partition;
    }

    public static class StratifiedQueryKeySelector<Q extends Serializable, P extends Serializable> implements KeySelector<StratifiedQuery<Q,P>, P> {
        @Override
        public P getKey(StratifiedQuery<Q,P> query) throws Exception {
            return query.getPartition();
        }
    }
}
