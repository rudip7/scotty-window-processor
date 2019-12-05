package Tests;


import Synopsis.Wavelets.WaveletSynopsis;

public class waveletTest {
    public static void main(String[] args) throws Exception {

        WaveletSynopsis waveletSynopsis = new WaveletSynopsis(6);
        waveletSynopsis.update(9);
        waveletSynopsis.update(3);
        System.out.println(waveletSynopsis);
        waveletSynopsis.update(9);
        waveletSynopsis.update(-5);
        System.out.println(waveletSynopsis);
        waveletSynopsis.update(5);
        waveletSynopsis.update(13);
        System.out.println(waveletSynopsis);
        waveletSynopsis.update(13);
        waveletSynopsis.update(17);
        System.out.println(waveletSynopsis);
        waveletSynopsis.update(14);
        waveletSynopsis.update(-2);
        System.out.println(waveletSynopsis);
        waveletSynopsis.update(9);
        waveletSynopsis.update(7);
        System.out.println(waveletSynopsis);
        waveletSynopsis.update(7);
        waveletSynopsis.update(3);
        System.out.println(waveletSynopsis);

        waveletSynopsis.padding();
        System.out.println(waveletSynopsis);

        for (int i = 0; i < 14; i++) {
            System.out.println(waveletSynopsis.pointQuery(i));
        }

    }
}
