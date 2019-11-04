package Synopsis.Sketches.HashFunctions;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.BitSet;

/**
 * Implementation of the EH3_HashFunction random bit generator
 *
 * @author joschavonhein
 */
public class EH3_HashFunction implements Serializable {
    private BitSet seed;
    private byte n; // input length in bits

    public BitSet getSeed() {
        return seed;
    }

    public byte getN() {
        return n;
    }

    /**
     *
     * @param seed  has to have n+1 bits
     * @param n     input length in bits
     */
    public EH3_HashFunction(BitSet seed, byte n) {
        this.seed = seed;
        this.n = n;
    }

    /**
     * Random number generator. Returns either true or false, depending on input and seed.
     * Based on the EH3_HashFunction scheme.
     * @param input bits
     * @return  true =1 , false = 0
     */
    public boolean rand(BitSet input){
        boolean hash = h(input);
        input.set(n); // [1,i] -> concatenate 1 and input

        input.and(seed);    // logical AND with the seed

        boolean si = input.cardinality() % 2 == 1;  // equal to the sequential XOR of the input.and(seed) bits

        return si ^ hash;   // last XOR
    }

    /**
     * nonlinear function of the input bits
     * @param input bits
     * @return  true = 1, false = 0
     */
    private boolean h(BitSet input){
        boolean result = input.get(0);
        for (int i = 1; i < input.size(); i++) {
            if (i % 2 == 0){
                result = result ^ input.get(i);
            }else {
                result = result | input.get(i);
            }
        }
        return result;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(seed);
        out.writeByte(n);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        seed = (BitSet) in.readObject();
        n = in.readByte();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
