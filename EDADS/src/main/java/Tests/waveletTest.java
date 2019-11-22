package Tests;

import Synopsis.Wavelets.WaveletSynopsis;

public class waveletTest {
    public static void main(String[] args) throws Exception {

        WaveletSynopsis waveletSynopsis = new WaveletSynopsis(800, 100000);

        for (int i = 0; i < 8; i++) {
            waveletSynopsis.update(i);
        }

        for (int i = 0; i < 8; i++) {
            System.out.println("PointQuery of index " + i + " : " + waveletSynopsis.PointQuery(i));
        }
        System.out.println(waveletSynopsis.RangeSumQuery(2,4));
    }
}
