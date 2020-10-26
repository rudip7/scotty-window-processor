package Benchmark.Sources;

public class IP_Address {
    public int address;

    public IP_Address(int address) {
        this.address = address;
    }

    public int getAddress() {
        return address;
    }

    public void setAddress(int address) {
        this.address = address;
    }

    @Override
    public String toString() {

        int bitmask = 0Xff;
        int d = address & bitmask;
        int c = (address >>> 8) & bitmask;
        int b = (address >>> 16) & bitmask;
        int a = (address >>> 24) & bitmask;
        return a + "." + b + "." + c + "." + d;
    }
}
