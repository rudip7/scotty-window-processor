package Synopsis.Sketches;


import Synopsis.Sketches.HashFunctions.EfficientH3Functions;
import Synopsis.InvertibleSynopsis;
import Synopsis.Synopsis;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Implementation of classical Count-Min sketch to estimate the frequencies of the elements in a datastream.
 * This implementation uses a family of pairwise independent hash functions to update the counters of the
 * sketch.
 *
 * @param <T> the type of elements maintained by this sketch
 * @author Rudi Poepsel Lemaitre
 */
public class CountMinSketch<T> implements InvertibleSynopsis<T>, Serializable {

    private int width;
    private int height;
    private long seed;
    private int[][] array;
    private EfficientH3Functions hashFunctions;
    private int elementsProcessed;
//    private HashSet<T> Elements;


    /**
     * Construct a Count-Min sketch
     *
     * @param width  value range of the hash functions
     * @param height number of hash functions
     * @param seed   for the randomness of the hash functions
     */
    public CountMinSketch(Integer width, Integer height, Long seed) {
        this.width = width;
        this.height = height;
        this.seed = seed;
        array = new int[height][width];
        this.hashFunctions = new EfficientH3Functions(height, seed);
        this.elementsProcessed = 0;
//        this.Elements = new HashSet<>();
    }


    /**
     * Update each row of the sketch with a new element by increasing the count of the hash position by 1.
     *
     * @param element new incoming element
     */
    @Override
    public void update(T element) {
        int input;
        if (element instanceof Number){
            input = ((Number) element).intValue();
        }else {
            input = element.hashCode();
        }
        int[] indices = hashFunctions.hash(input);
        for (int i = 0; i < height; i++) {
            array[i][indices[i] % width]++;
        }
//        Elements.add(element);
        elementsProcessed++;
    }

//    /**
//     * Update the sketch with incoming positive weighted tuple.
//     *
//     * @param tuple
//     * @param weight must be positive
//     */
//    public void weightedUpdate(Object tuple, int weight) throws Exception {
//        if (weight < 0) {
//            throw new Exception("Count Min sketch only accepts positive weights!");
//        }
//        int[] indices = hashFunctions.hash(tuple);
//        for (int i = 0; i < height; i++) {
//            array[i][indices[i] % width] += weight;
//        }
//        elementsProcessed++;
//    }

    /**
     * Query the sketch and get an estimate of the count of this value.
     *
     * @param element to query the frequency
     * @return The approximate count of tuple so far
     */
    public Integer query(T element) {
        int input;
        if (element instanceof Number){
            input = ((Number) element).intValue();
        }else {
            input = element.hashCode();
        }
        int[] indices = hashFunctions.hash(input);
        int min = -1;
        for (int i = 0; i < height; i++) {
            if (min == -1)
                min = array[i][indices[i] % width];
            else if (array[i][indices[i] % width] < min) {
                min = array[i][indices[i] % width];
            }
        }
        return min;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public int[][] getArray() {
        return array;
    }

    public int getElementsProcessed() {
        return elementsProcessed;
    }

    public void setArray(int[][] array) {
        if (!(array.length == height && array[0].length == width)) {
            throw new IllegalArgumentException("Synopsis.Sketches have to be the same size");
        }
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                this.array[i][j] = array[i][j];
            }
        }
    }

    public void setElementsProcessed(int elementsProcessed) {
        this.elementsProcessed = elementsProcessed;
    }
    //    public HashSet<T> getElements() {
//        return Elements;
//    }

    /**
     * Function to Merge two Count-Min sketches by adding their counters.
     *
     * @param other Count-Min sketch to be merged with
     * @return merged Count-Min sketch
     * @throws Exception
     */
    @Override
    public CountMinSketch merge(Synopsis<T> other) {
        if (other instanceof CountMinSketch) {
            CountMinSketch otherCM = (CountMinSketch) other;
            if (otherCM.getWidth() == width && otherCM.getHeight() == height && hashFunctions.equals(otherCM.hashFunctions)) {
                int[][] a2 = otherCM.getArray();
                for (int i = 0; i < height; i++) {
                    for (int j = 0; j < width; j++) {
                        array[i][j] += a2[i][j];
                    }
                }
                elementsProcessed += otherCM.getElementsProcessed();
//                Elements.addAll(otherCM.getElements());
                return this;
            }
        }
        throw new IllegalArgumentException("Synopsis.Sketches to merge have to be the same size and hash Functions");
    }

//    @Override
//    public CountMinSketch clone() {
//        CountMinSketch cm = new CountMinSketch(width, height, seed);
//        cm.setElementsProcessed(this.elementsProcessed);
//        cm.setArray(this.array);
//
//        return cm;
//    }

    @Override
    public void decrement(T toDecrement) {
        int input;
        if (toDecrement instanceof Number){
            input = ((Number) toDecrement).intValue();
        }else {
            input = toDecrement.hashCode();
        }
        int[] indices = hashFunctions.hash(input);
        for (int i = 0; i < height; i++) {
            array[i][indices[i] % width]--;
        }
        elementsProcessed--;
    }

    @Override
    public CountMinSketch<T> invert(InvertibleSynopsis<T> other) {
        if (other instanceof CountMinSketch) {
            CountMinSketch otherCM = (CountMinSketch) other;
            if (otherCM.getWidth() == width && otherCM.getHeight() == height && hashFunctions.equals(otherCM.hashFunctions)) {
                int[][] a2 = otherCM.getArray();
                for (int i = 0; i < height; i++) {
                    for (int j = 0; j < width; j++) {
                        array[i][j] -= a2[i][j];
                    }
                }
                elementsProcessed -= otherCM.getElementsProcessed();
                return this;
            }
        }
        throw new IllegalArgumentException("Synopsis.Sketches to invert have to be the same size and hash Functions");
    }


    @Override
    public String toString() {
        return "CountMinSketch{" +
                "width=" + width +
                ", height=" + height +
                ", seed=" + seed +
                ", array=" + Arrays.toString(array) +
                ", hashFunctions=" + hashFunctions +
                ", elementsProcessed=" + elementsProcessed +
                '}';
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(width);
        out.writeInt(height);
        out.writeLong(seed);
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                out.writeInt(array[i][j]);
            }
        }
        out.writeObject(hashFunctions);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        width = in.readInt();
        height = in.readInt();
        seed = in.readLong();
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                array[i][j] = in.readInt();
            }
        }
        hashFunctions = (EfficientH3Functions) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }


}
