package Tests;

/**
 * @author Zahra Salmani
 */

import Synopsis.Histograms.SplitAndMergeWithDDSketch;
import Synopsis.Sketches.DDSketch;
import Synopsis.Histograms.SplitAndMergeWithBackingSample;
import Synopsis.MergeableSynopsis;
import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
public class SMWithBckSampleTest {
    /*
    public void updateTest(){
        SplitAndMergeWithBackingSample SMWithBckSample= new SplitAndMergeWithBackingSample(10,0.1,40);
        //read from file and update with read elements
        File file= new File("data/dataset.csv");
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                SMWithDDSketch.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
    */

}
