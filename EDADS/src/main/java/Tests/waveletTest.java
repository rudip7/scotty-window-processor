package Tests;


import Synopsis.Wavelets.SiblingTree;

public class waveletTest {
    public static void main(String[] args) throws Exception {

        SiblingTree siblingTree = new SiblingTree(20);
        System.out.println(siblingTree.toString());
        siblingTree.climbup(9,3);
        System.out.println(siblingTree.toString());
        siblingTree.climbup(9, -5);
        System.out.println(siblingTree.toString());
        siblingTree.climbup(5, 13);
        System.out.println(siblingTree.toString());
        siblingTree.climbup(13, 17);
        siblingTree.discard();
        siblingTree.discard();
        System.out.println(siblingTree.toString());
        siblingTree.climbup(14, -2);
        siblingTree.discard();
        siblingTree.discard();
        System.out.println(siblingTree.toString());
        siblingTree.climbup(9, 7);
        siblingTree.discard();
        siblingTree.discard();
        System.out.println(siblingTree.toString());
        siblingTree.climbup(7, 3);
        siblingTree.discard();
        siblingTree.discard();
        System.out.println(siblingTree.toString());
    }
}
