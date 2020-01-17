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
        BarSplittingHistogram BASHistogram= new BarSplittingHistogram(9,10);
        //read from file and update with read elements
        File file= new File("data/dataset.csv");
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

        //check the frequency of each bar,
        TreeMap<Integer, Float> bars=BASHistogram.getBars();
        Assertions.assertTrue(bars.size()<=90);
        for(Map.Entry<Integer,Float> element:bars.entrySet()){

                Assertions.assertTrue(element.getValue()<= 75.56);

        }

        EquiDepthHistogram equiDepthHistogram= BASHistogram.buildEquiDepthHistogram();

        double errorbound= (4.0/BASHistogram.getP())*(BASHistogram.getTotalFrequencies()/BASHistogram.getNumBuckets());
        // double [] boundries = equiDepthHistogram.getLeftBoundaries();
//        Assertions.assertTrue(Math.abs(481-equiDepthHistogram.rangeQuery(0.0, 29.06908892805006)) <=errorbound);
//        Assertions.assertTrue(Math.abs(400-equiDepthHistogram.rangeQuery(29.06908892805006, 54.43088446195934) )<=errorbound);
//        Assertions.assertTrue(Math.abs(425-equiDepthHistogram.rangeQuery(54.43088446195934, 79.34602667515135)) <=errorbound);
//        Assertions.assertTrue(Math.abs(408-equiDepthHistogram.rangeQuery(79.34602667515135, 104.54059090481564)) <=errorbound);
//        Assertions.assertTrue(Math.abs(410-equiDepthHistogram.rangeQuery(104.54059090481564, 128.16725341562798) )<=errorbound);
//        Assertions.assertTrue(Math.abs(385-equiDepthHistogram.rangeQuery(128.16725341562798, 148.6070893468689)) <=errorbound);
//        Assertions.assertTrue(Math.abs(404-equiDepthHistogram.rangeQuery(148.6070893468689, 172.38918153458843) )<=errorbound);
//        Assertions.assertTrue(Math.abs(403-equiDepthHistogram.rangeQuery(172.38918153458843, 199.22379903590425)) <=errorbound);
//        Assertions.assertTrue(Math.abs(384-equiDepthHistogram.rangeQuery(199.22379903590425, 222.9701722420302) )<=errorbound);
//        Assertions.assertTrue(Math.abs(300-equiDepthHistogram.rangeQuery(222.9701722420302, 239) )<=errorbound);

        Assertions.assertTrue(Math.abs(403-equiDepthHistogram.rangeQuery(0.0, 24.770805301880852)) <=errorbound);
        Assertions.assertTrue(Math.abs(396-equiDepthHistogram.rangeQuery(24.770805301880852, 50.74404908720416) )<=errorbound);
        Assertions.assertTrue(Math.abs(413-equiDepthHistogram.rangeQuery(50.74404908720416, 74.03145461751883)) <=errorbound);
        Assertions.assertTrue(Math.abs(391-equiDepthHistogram.rangeQuery(74.03145461751883, 97.78014234004232)) <=errorbound);
        Assertions.assertTrue(Math.abs(406-equiDepthHistogram.rangeQuery(97.78014234004232, 121.633294864397) )<=errorbound);
        Assertions.assertTrue(Math.abs(409-equiDepthHistogram.rangeQuery(121.633294864397, 143.33745009315945)) <=errorbound);
        Assertions.assertTrue(Math.abs(392-equiDepthHistogram.rangeQuery(143.33745009315945, 167.49265477439664) )<=errorbound);
        Assertions.assertTrue(Math.abs(400-equiDepthHistogram.rangeQuery(167.49265477439664, 192.2378206976074)) <=errorbound);
        Assertions.assertTrue(Math.abs(401-equiDepthHistogram.rangeQuery(192.2378206976074, 216.18081861728064) )<=errorbound);
        Assertions.assertTrue(Math.abs(389-equiDepthHistogram.rangeQuery(216.18081861728064, 239) )<=errorbound);


    }

    @Test
    public void mergeTest()
    {
        BarSplittingHistogram BASHistogram= new BarSplittingHistogram(9,10);
        //read from file and update with read elements
        File file= new File("data/data.csv");
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

        BarSplittingHistogram otherBASHistogram= new BarSplittingHistogram(4,8);
        //read from file and update with read elements
        File ofile= new File("data/testdata.csv");
        Scanner oinputStream;
        try{
            oinputStream = new Scanner(ofile);
            while(oinputStream.hasNext()){
                String oline= oinputStream.next();
                otherBASHistogram.update(Integer.parseInt(oline));
            }
            oinputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        BarSplittingHistogram mergeResultHistogram=BASHistogram.merge(otherBASHistogram);
        EquiDepthHistogram equiDepthHistogram= mergeResultHistogram.buildEquiDepthHistogram();

        double mergeTotalFrequency= BASHistogram.getTotalFrequencies()+otherBASHistogram.getTotalFrequencies();
        double errorbound= (4.0/BASHistogram.getP())*(mergeTotalFrequency/BASHistogram.getNumBuckets());
        System.out.println(BASHistogram.getTotalFrequencies());
        System.out.println(equiDepthHistogram);

    }

}
