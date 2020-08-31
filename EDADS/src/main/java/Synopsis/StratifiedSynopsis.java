package Synopsis;

@Deprecated // use StratifiedSynopsisWrapper instead
public abstract class StratifiedSynopsis<Partition>{
    private Partition partitionValue = null; //the value of partition in a startiefied sampling.

    /**
     * @return partitionValue
     */
    public Partition getPartitionValue() {
        return partitionValue;
    }

    public void setPartitionValue(Partition partitionValue) {
        if (this.partitionValue == null) {
            this.partitionValue = partitionValue;
        }
    }
}
