package Tests;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import Synopsis.Histograms.BarSplittingHistogram;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * @author Zahra Salmani
 */
public class BarSplittingHistogramTest {

    @Test
    public void updateTest()
    {
        int count=0;
        BarSplittingHistogram BASHistogram= new BarSplittingHistogram(7,100);
        File file= new File("data/data.csv");
        // this gives you a 2-dimensional array of strings
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                BASHistogram.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        TreeMap<Integer, Float> bars=BASHistogram.getBars();
        Assertions.assertTrue(bars.size()<= 400);
        //System.out.println(bars.keySet());
//        int [] valuesWithHighFrequency=new int []{1,2,4,8,11,14,16,17,29,33,36,37,39,41,44,47,48,51,52,53,54,58,
//                59,60,62,68,70,71,72,76,77,78,80,82,83,84,88,89,90,97,99,104,106,107,112,113,114,117,118,121,122,
//                128,132,133,134,135,136,137,138,140,141,143,147,151,154,160,162,165,167,168,170,172,175,179,185,
//                188,189,193,197,198,201,205,206,207,211,212,213,215,220,223,225,226,227,229,232,233,235,238};
//

        for(Map.Entry<Integer,Float> bar:bars.entrySet()){
//            if(!Arrays.stream(valuesWithHighFrequency).anyMatch(i -> i == bar.getKey())){
                if(bar.getValue()>17 ){
                   // Assertions.assertTrue(((bar.getValue()-17))/17<0.58);
                    System.out.println(bar.getKey());
                }
                //else{
                   // Assertions.assertTrue(bar.getValue()<17);
//                }


//            }
        }

//      double l= BASHistogram.buildEquiDepthHistogram().rangeQuery(8.038455465139537, 9.91700718565135);
//        double n= BASHistogram.buildEquiDepthHistogram().rangeQuery(154.89992181391713, 157.3875236294896);
//        double m= BASHistogram.buildEquiDepthHistogram().rangeQuery(237.8,239);
//        System.out.println(BASHistogram.buildEquiDepthHistogram());
//        System.out.println("----------------");
//        System.out.println(l);
//        System.out.println(n);
//        System.out.println(m);
    }
}
