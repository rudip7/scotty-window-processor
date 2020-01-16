package Synopsis;

public abstract class StratifiedSynopsis<T> {
    private T partitionValue = null;

    public T getPartitionValue() {
        return partitionValue;
    }

    public void setPartitionValue(T partitionValue) {
        if (this.partitionValue == null) {
            this.partitionValue = partitionValue;
        }
    }
}
