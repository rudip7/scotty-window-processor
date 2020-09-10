package Synopsis.Sketches.HashFunctions;

import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.Arrays;
import java.util.Objects;

public class EfficientH3Functions {
    private int[][] q_matrices;
    private int numFunctions;
    private long seed;

    //Logger logger = LoggerFactory.getLogger(EfficientH3Functions.class);

    /**
     * constructor- set q_matrices with randomly generated numbers
     *
     * @param numFunctions    the amount of eh3 hash_functions
     * @param seed      seed for the random number generator
     */
    public EfficientH3Functions(int numFunctions, long seed) {
        this.numFunctions = numFunctions;
        this.seed = seed;
        q_matrices = new int[numFunctions][32]; // initialize numFunctions, n * H matrices
        XORShiftRandom random = new XORShiftRandom(seed);

        for (int a = 0; a < numFunctions; a++) {
            for (int i = 0; i < 32; i++) {
                q_matrices[a][i] = random.nextInt();   // set the q_matrices
            }
        }
    }

    /**
     * Generates Hash values with size H
     *
     * @param input value which is used to generate the hashes - should not be 0!
     * @return Hash values as Long Array
     */
    public int[] hash(int input) {
        // input = input == 0 ? 1 : input; // input must not be 0
        int[] result = new int[numFunctions];
        int inputCopy = input;
        for (int i = 0; i < numFunctions; i++) {
            int temp = input;
            int current = 0;
            for (int j = 0; j < 32; j++) {
                current = current ^ ((1 & inputCopy) * q_matrices[i][j]);
                inputCopy >>>= 1;

            }
            if (current < 0) {
                result[i] = -1 * current;
            } else {
                result[i] = current;
            }
            inputCopy = input;
        }
        return result;
    }

    /**
     * return if two EfficientH3Functions are equal or not
     * @param o
     * @return false if they are not equal and True if they equal (based on numFunctions and seed)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EfficientH3Functions)) return false;
        EfficientH3Functions that = (EfficientH3Functions) o;
//        if (numFunctions == that.numFunctions) {
//            if (seed == that.seed) {
//                for (int i = 0; i < this.q_matrices.length; i++) {
//                    for (int j = 0; j < this.q_matrices[0].length; j++) {
//                        if (this.q_matrices[i][j] != that.q_matrices[i][j]){
//                            Environment.out.println("DAFUQ");
//                        }
//                    }
//                }
//            }
//        }
        return numFunctions == that.numFunctions &&
                seed == that.seed;
    }

    /**
     * convert the information contained in the object .
     * could be used to print the EfficientH3Functions.
     *
     * @return a string of contained information
     */
    @Override
    public String toString() {
        return "EfficientH3Functions{" +
                "numFunctions=" + numFunctions +
                ", seed=" + seed +
                '}';
    }

    /**
     * Returns a hash code based on the numFunctions,seeds and q_matrices.
     *
     * @return
     */
    @Override
    public int hashCode() {
        int result = Objects.hash(numFunctions, seed);
        result = 31 * result + Arrays.hashCode(q_matrices);
        return result;
    }

    /**
     * Method needed for Serializability.
     * write object to an output Stream
     * @param out, output stream to write object to
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(q_matrices);
        out.writeInt(numFunctions);
        out.writeLong(seed);
    }

    /**
     * Method needed for Serializability.
     * read object from an input Stream
     * @param in, input stream to read from
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        q_matrices = (int[][]) in.readObject();
        numFunctions = in.readInt();
        seed = in.readLong();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
