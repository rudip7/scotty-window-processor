package Tests;


import Synopsis.Wavelets.SiblingTree;

public class waveletTest {
    public static void main(String[] args) throws Exception {

        SiblingTree siblingTree = new SiblingTree(20);

        for (int i = 0; i < 8; i++) {
            waveletSynopsis.update(i);
        }

        for (int i = 0; i < 8; i++) {
            System.out.println("PointQuery of index " + i + " : " + waveletSynopsis.PointQuery(i));
        }
        System.out.println(waveletSynopsis.RangeSumQuery(2,4));
    }
}
