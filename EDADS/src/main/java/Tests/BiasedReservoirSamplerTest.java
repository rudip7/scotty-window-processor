package Tests;
import  Synopsis.Sampling.BiasedReservoirSampler;
import Synopsis.Sampling.SampleElement;
import org.junit.jupiter.api.Assertions;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Test;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;
import java.io.FileNotFoundException;
/**
 * @author Zahra Salmani
 */

public class BiasedReservoirSamplerTest {
    @Test
    public void updateTest() {
        BiasedReservoirSampler bReservoirSampler= new BiasedReservoirSampler(10);

        //read data from file
        String fileName= "data/testdata.csv";
        File file= new File(fileName);
        ArrayList<String> lines = new ArrayList<>();
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                lines.add(line);
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //update sampler from file and stop before it cause replacement( less than sample size)
        for (int i=0;i<10;i++) {
            bReservoirSampler.update(new SampleElement(Integer.parseInt(lines.get(i)),i+3));
        }

        // create the sample that we expect after update
        ArrayList<SampleElement> fixedSample =  new ArrayList(Arrays.asList( new SampleElement(103, 3),
                new SampleElement(52, 4), new SampleElement(161, 5),
                new SampleElement(25, 6), new SampleElement(188, 7),
                new SampleElement(19, 8), new SampleElement(48, 9),
                new SampleElement(93, 10), new SampleElement(50, 11),
                new SampleElement(143, 12)));
        //check expected and actual samples
        ArrayList<SampleElement> notFullSample=  new ArrayList(Arrays.asList(bReservoirSampler.getSample()));
        Assert.assertTrue(notFullSample.equals(fixedSample));

        //add an element with timestamp earlier than last element in sampler

        bReservoirSampler.update( new SampleElement(12, 2));
        ArrayList<SampleElement> addEarlierElSample=  new ArrayList(Arrays.asList(bReservoirSampler.getSample()));
        Assert.assertTrue(addEarlierElSample.equals(fixedSample));

        //update sampler from file and continue after exceeding sample size
        for (int i=10;i<lines.size();i++) {
            bReservoirSampler.update(new SampleElement(Integer.parseInt(lines.get(i)),i));
        }
        ArrayList<SampleElement> fullSample = new ArrayList(Arrays.asList(bReservoirSampler.getSample()));
        //check that the sample has changed, because should give priority to new arrivals
        Assert.assertThat(fullSample, new IsNot(new IsEqual(fixedSample)));
    }

    @Test
    public void mergeTest() throws Exception {

        BiasedReservoirSampler reservoirMergeWithEmpty= new BiasedReservoirSampler(15);
        BiasedReservoirSampler otherWithDifferentSize= new BiasedReservoirSampler(12);
        Assertions.assertThrows(IllegalArgumentException.class,()->reservoirMergeWithEmpty.merge(otherWithDifferentSize)); //merge 2 sampler with different size results in exceptions

        ArrayList<SampleElement> sample1 =  new ArrayList(Arrays.asList( new SampleElement(103, 10),
                new SampleElement(52, 13), new SampleElement(188, 17),
                new SampleElement(19, 25), new SampleElement(48, 29),
                new SampleElement(161, 35),new SampleElement(25, 65)));
        //update reservoirMergeWithEmpty with samples
        for (SampleElement el : sample1){
            reservoirMergeWithEmpty.update(el);
        }
        ArrayList<SampleElement> notMergedSample= new ArrayList(Arrays.asList(reservoirMergeWithEmpty.getSample().clone()));
        //construct other sampler
        BiasedReservoirSampler other= new BiasedReservoirSampler(15);
        //merge reservoir with other that is empty
        BiasedReservoirSampler mergeEmpty=reservoirMergeWithEmpty.merge(other);
        ArrayList<SampleElement> mergeEmptySample=  new ArrayList(Arrays.asList(mergeEmpty.getSample()));
        //check merge with empty did not effect sample
        Assert.assertTrue(notMergedSample.equals(mergeEmptySample));
        Assertions.assertEquals(mergeEmpty.getMerged(),2);

        //add element to other but not as many as to full merge sample
        BiasedReservoirSampler otherNotFull= new BiasedReservoirSampler(10);
        BiasedReservoirSampler reservoirMergeNotFull= new BiasedReservoirSampler(10);
        for (SampleElement el : sample1){
            reservoirMergeNotFull.update(el);
        }
        ArrayList<SampleElement> sampleOtherFirst =  new ArrayList(Arrays.asList( new SampleElement(45, 7),
                new SampleElement(34, 9),new SampleElement(45, 47)));
        for (SampleElement el : sampleOtherFirst){
            otherNotFull.update(el);
        }
        BiasedReservoirSampler mergeNotFull=reservoirMergeNotFull.merge(otherNotFull);
        ArrayList<SampleElement> mergeNotFullSample=  new ArrayList(Arrays.asList(mergeNotFull.getSample()));
        ArrayList<SampleElement> notFullSample =  new ArrayList(Arrays.asList( new SampleElement(45, 7),
                new SampleElement(34, 9),new SampleElement(103, 10),
                new SampleElement(52, 13), new SampleElement(188, 17),
                new SampleElement(19, 25), new SampleElement(48, 29),
                new SampleElement(161, 35),new SampleElement(45, 47)
                ,new SampleElement(25, 65)));

        //check merge with empty did not effect sample
        Assert.assertTrue(notFullSample.equals(mergeNotFullSample));
        Assertions.assertEquals(mergeNotFull.getMerged(),2);

        //update a new reservoir
        BiasedReservoirSampler reservoir= new BiasedReservoirSampler(15);
        for (SampleElement el : sample1){
            reservoir.update(el);
        }

        ArrayList<SampleElement> sampleOtherSecond =  new ArrayList(Arrays.asList(  new SampleElement(45, 7),
                new SampleElement(34, 9),new SampleElement(12, 37),
                new SampleElement(25, 45), new SampleElement(18, 67),
                new SampleElement(189, 70), new SampleElement(40, 88),
                new SampleElement(293, 105), new SampleElement(54, 115),
                new SampleElement(143, 125)));
        //update other with some elements
        for (SampleElement el : sampleOtherSecond){
            other.update(el);
        }

       BiasedReservoirSampler mergeResult=reservoir.merge(other);//merge reservoir and non-empty other
       ArrayList<SampleElement> mergeResultSample=  new ArrayList(Arrays.asList(mergeResult.getSample()));

       //compare expected result and actual one from merge result
        Assertions.assertTrue(mergeResultSample.contains(new SampleElement(143, 125)));
        Assertions.assertEquals(mergeResult.getMerged(),2);
    }
}


