package Tests;

import Synopsis.Histograms.EquiDepthHistogram;
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
        BarSplittingHistogram BASHistogram= new BarSplittingHistogram(9,10);
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
        System.out.println(BASHistogram.getBars().size());
        //Assertions.assertTrue(BASHistogram.getBars().size()<=90);
        EquiDepthHistogram equiDepthHistogram= BASHistogram.buildEquiDepthHistogram();
        double errorbound= (4.0/BASHistogram.getP())*(BASHistogram.getTotalFrequencies()*BASHistogram.getNumBuckets());
        double [] boundries = equiDepthHistogram.getLeftBoundaries();


        Assertions.assertTrue(Math.abs(481-equiDepthHistogram.rangeQuery(0.0, 29.06908892805006)) <=errorbound);
        Assertions.assertTrue(Math.abs(400-equiDepthHistogram.rangeQuery(29.06908892805006, 54.43088446195934) )<=errorbound);
        Assertions.assertTrue(Math.abs(425-equiDepthHistogram.rangeQuery(54.43088446195934, 79.34602667515135)) <=errorbound);
        Assertions.assertTrue(Math.abs(408-equiDepthHistogram.rangeQuery(79.34602667515135, 104.54059090481564)) <=errorbound);
        Assertions.assertTrue(Math.abs(410-equiDepthHistogram.rangeQuery(104.54059090481564, 128.16725341562798) )<=errorbound);
        Assertions.assertTrue(Math.abs(385-equiDepthHistogram.rangeQuery(128.16725341562798, 148.6070893468689)) <=errorbound);
        Assertions.assertTrue(Math.abs(404-equiDepthHistogram.rangeQuery(148.6070893468689, 172.38918153458843) )<=errorbound);
        Assertions.assertTrue(Math.abs(403-equiDepthHistogram.rangeQuery(172.38918153458843, 199.22379903590425)) <=errorbound);
        Assertions.assertTrue(Math.abs(384-equiDepthHistogram.rangeQuery(199.22379903590425, 222.9701722420302) )<=errorbound);
        Assertions.assertTrue(Math.abs(300-equiDepthHistogram.rangeQuery(222.9701722420302, 239) )<=errorbound);



        System.out.println(BASHistogram.buildEquiDepthHistogram());

    }
}
