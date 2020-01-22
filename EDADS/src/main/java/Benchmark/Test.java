package Benchmark;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.api.java.tuple.Tuple11;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

public class Test {
    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
    public static void main(String[] args) {
//        String dataFilePath = "C:\\Users\\Rudi\\Documents\\EDADS\\flink-training-exercises\\data\\nycTaxiRides.gz";
//        try {
//            GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
//            BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));
//
//            long number = 0;
//            String line;
//            TreeMap<Long, Integer> counts = new TreeMap<>();
//            TreeMap<Long, Integer> times = new TreeMap<>();
//            int des = 0;
//            Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> old = null;
//            while(reader.ready() && (line = reader.readLine()) != null) {
//                // read first ride
//                String[] tokens = line.split(",");
//                Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> ride = fromString(line);
//                if (old != null && getEventTime(old) > getEventTime(ride)){
//                    des ++;
//                }
//
//                old = ride;
//                counts.merge(ride.f1, 1, (a, b) -> a + b);
//                long diff = (getEventTime(ride)-1356998400000L);
//                long key = diff%207080300L;
////                long ref = diff/20708030L; 16 Buck
//                long ref = diff/(20708030L/3);
//                times.merge(ref, 1, (a, b) -> a + b);
////                times.merge(getEventTime(ride), 1, (a, b) -> a + b);
//                number++;
//            }
//            System.out.println(number);
//            System.out.println("DES:  "+des);
////            System.out.println(counts.toString());
//            System.out.println(times.toString());
//            System.out.println(times.size());
//            System.out.println(times.firstKey()-times.lastKey());
//
//            System.out.println(times.firstKey());
//            System.out.println(times.lastKey());
//
//
//            int check=0;
//            for (Integer c:
//                 counts.values()) {
//                check+=c ;
//            }
//            System.out.println(check);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        String dataFilePath = "C:\\Users\\Rudi\\Documents\\EDADS\\flink-training-exercises\\data\\uniformTimestamped.gz";
        try {
            GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

            long number = 0;
            String line;
            Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> old = null;
            while(reader.ready() && (line = reader.readLine()) != null) {
                // read first ride
                String[] tokens = line.split(",");
                number++;
            }
            System.out.println(number);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 11) {
            throw new RuntimeException("Invalid record: " + line);
        }

        Tuple11 ride = new Tuple11();

        try {
            ride.f0 = Long.parseLong(tokens[0]);

            switch (tokens[1]) {
                case "START":
                    ride.f3 = true;
                    ride.f4 = DateTime.parse(tokens[2], timeFormatter).getMillis();
                    ride.f5 = DateTime.parse(tokens[3], timeFormatter).getMillis();
                    break;
                case "END":
                    ride.f3 = false;
                    ride.f5 = DateTime.parse(tokens[2], timeFormatter).getMillis();
                    ride.f4 = DateTime.parse(tokens[3], timeFormatter).getMillis();
                    break;
                default:
                    throw new RuntimeException("Invalid record: " + line);
            }

            ride.f6 = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
            ride.f7 = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.f8 = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.f9 = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
            ride.f10 = Short.parseShort(tokens[8]);
            ride.f1 = Long.parseLong(tokens[9]);
            ride.f2 = Long.parseLong(tokens[10]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    public static long getEventTime(Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> ride) {
        if (ride.f3) {
            return ride.f4;
        } else {
            return ride.f5;
        }
    }
}
