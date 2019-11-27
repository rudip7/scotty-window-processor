package Tests;
import Synopsis.Sampling.FiFoSampler;
import Synopsis.Sampling.SampleElement;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;

import org.junit.jupiter.api.Assertions;
/**
 * @author Zahra Salmani
 */
public class FiFoSamplerTest {
     @Test
   public void updateTest() {
        FiFoSampler fifoSampler = new FiFoSampler(10);

        String fileName = "data/testdata.csv";
        File file = new File(fileName);

        // this gives you a 2-dimensional array of strings
        LinkedList<String> lines = new LinkedList<>();
        Scanner inputStream;

        try {
            inputStream = new Scanner(file);
            while (inputStream.hasNext()) {
                String line = inputStream.next();
                // this adds the currently parsed line to the 2-dimensional string array
                lines.add(line);
            }
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 10; i++) {
            SampleElement sampleElement=new SampleElement(lines.get(i),i);
            fifoSampler.update(sampleElement);
        }

        SampleElement[] Sample1= new SampleElement[]{new SampleElement(103,0), new SampleElement(52,1), new SampleElement(161,2),
                new SampleElement(25,3), new SampleElement(188, 4),new SampleElement(19, 5),new SampleElement(48,6),
                        new SampleElement(93,7), new SampleElement(50, 8),new SampleElement(143,9)};
         TreeSet<SampleElement> notFullExpectedSample = new TreeSet<>();
         for (SampleElement el : Sample1) {
             notFullExpectedSample.add(el);
         }

         Object notFullSample = fifoSampler.getSample();
        Assert.assertEquals(notFullSample, notFullExpectedSample);

        for (int i=10;i<lines.size();i++) {
            SampleElement sampleElement=new SampleElement(lines.get(i),i);
            fifoSampler.update(sampleElement);
        }
         TreeSet<SampleElement> fullSample = fifoSampler.getSample();

         SampleElement[] Sample2= new SampleElement[]{new SampleElement(87,90), new SampleElement(198,91), new SampleElement(106,92),
                 new SampleElement(34,93), new SampleElement(6, 94),new SampleElement(153, 95),new SampleElement(168,96),
                 new SampleElement(90,97), new SampleElement(55, 98),new SampleElement(59,99)};
         TreeSet<SampleElement> fullExpectedSample = new TreeSet<>();
         for (SampleElement el : Sample2) {
             fullExpectedSample.add(el);
         }


        Assert.assertEquals(fullSample, fullExpectedSample);
    }

    @Test
    public void mergeTest() throws Exception {
        FiFoSampler fifoSampler = new FiFoSampler(12);
        FiFoSampler other = new FiFoSampler(10);
        Assertions.assertThrows(IllegalArgumentException.class,()->fifoSampler.merge(other));
    }



}




