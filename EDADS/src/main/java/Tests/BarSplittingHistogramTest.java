package Tests;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import Synopsis.Histograms.BarSplittingHistogram;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

/**
 * @author Zahra Salmani
 */
public class BarSplittingHistogramTest {

    @Test
    public void updateTest()
    {
        int count=0;
        BarSplittingHistogram BASHistogram= new BarSplittingHistogram(4,100);
        File file= new File("data/dataset.csv");
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

        for(Map.Entry<Integer,Float> bar:bars.entrySet()){
            if(bar.getValue()>17){
                //System.out.println(bar.getKey());

            }
        }
        System.out.println(count);
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
