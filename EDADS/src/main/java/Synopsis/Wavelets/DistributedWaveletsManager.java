package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsis;

public class DistributedWaveletsManager<Input> extends NonMergeableSynopsis<Input, Wavelets<Input>> {

    @Override
    public int getSynopsisIndex(int streamIndex) {
        return 0;
    }
}
