package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsis;

public class SliceWaveletsManager<Input> extends NonMergeableSynopsis<Input, DistributedWaveletsManager<Input>> {

    @Override
    public int getSynopsisIndex(int streamIndex) {
        return 0;
    }
}
