package Synopsis.Wavelets;

import Synopsis.Synopsis;

public class Wavelets<T> implements Synopsis<T> {

    SiblingTree siblingTree;
    int elementCounter = 0;
    double data1;
    int size;

    public Wavelets(int size) {
        siblingTree = new SiblingTree(size);
    }

    @Override
    public void update(T element) {
        if (element instanceof Number){
            elementCounter++;
            if (elementCounter % 2 == 0){
                double data2 = ((Number) element).doubleValue();
                siblingTree.climbup(data1, data2);
                if (elementCounter > size){
                    siblingTree.discard();
                    siblingTree.discard();
                }
            }else {
                data1 = ((Number) element).doubleValue();
            }
        }else {
            throw new IllegalArgumentException("input elements have to be instance of Number!");
        }
    }
}
