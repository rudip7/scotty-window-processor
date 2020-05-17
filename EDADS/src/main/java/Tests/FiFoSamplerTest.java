package Tests;
import Synopsis.Sampling.FiFoSampler;
import Synopsis.Sampling.SampleElement;
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

        // read element from files before it exceeds sample size
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
        //create desired sample when fifosampler is not full yet
        SampleElement[] Sample1= new SampleElement[]{new SampleElement("103",0), new SampleElement("52",1),
                new SampleElement("161",2),
                new SampleElement("25",3), new SampleElement("188", 4),new SampleElement("19", 5),
                new SampleElement("48",6),
                new SampleElement("93",7), new SampleElement("50", 8),new SampleElement("143",9)};


         TreeSet<SampleElement> notFullExpectedSample = new TreeSet<>();
         for (SampleElement el : Sample1) {
             notFullExpectedSample.add(el);
         }

         TreeSet<SampleElement>  notFullSample = fifoSampler.getSample();
         // compare desired and actual samples
         Assertions.assertTrue(sampleTreeSetComprator(notFullSample,notFullExpectedSample));


         //the same steps as above but this time read more elements than sample size, so sampler will remove older elements
        for (int i=10;i<lines.size();i++) {
            SampleElement sampleElement=new SampleElement(lines.get(i),i);
            fifoSampler.update(sampleElement);
        }
         TreeSet<SampleElement> fullSample = fifoSampler.getSample();
         // desired full sample
         SampleElement[] Sample2= new SampleElement[]{new SampleElement("87",90), new SampleElement("198",91),
                 new SampleElement("106",92),
                 new SampleElement("34",93), new SampleElement("6", 94),
                 new SampleElement("153", 95),new SampleElement("168",96),
                 new SampleElement("90",97), new SampleElement("55", 98),new SampleElement("59",99)};
         TreeSet<SampleElement> fullExpectedSample = new TreeSet<>();
         for (SampleElement el : Sample2) {
             fullExpectedSample.add(el);
         }

        Assertions.assertTrue(sampleTreeSetComprator(fullExpectedSample,fullSample));

     }
    @Test
    public void mergeWithEmptyTest() throws Exception {
        FiFoSampler fifoSampler = new FiFoSampler(10);//main fifo sampler other will be merged to.
        FiFoSampler otherWithSize = new FiFoSampler(12);// other sampler with different size should throws exception
        Assertions.assertThrows(IllegalArgumentException.class,()->fifoSampler.merge(otherWithSize));

        FiFoSampler otherMergedInEmpty = new FiFoSampler(10);// when main sampler is Empty and the other is full

        //update other fifosampler with sample elements and keep main sampler empty
        SampleElement[] Sample1= new SampleElement[]{new SampleElement("87",1), new SampleElement("198",5),
                new SampleElement("106",11),
                new SampleElement("34",12), new SampleElement("6", 17),
                new SampleElement("153", 20),new SampleElement("168",23),
                new SampleElement("90",24), new SampleElement("55", 28),new SampleElement("39",32)};

        for (SampleElement el : Sample1) {
            otherMergedInEmpty.update(el);
        }

        TreeSet<SampleElement> mergeMainEmptyExpectedTree = (TreeSet<SampleElement>)otherMergedInEmpty.getSample().clone();//
        TreeSet<SampleElement> mergemainEmptyTree = fifoSampler.merge(otherMergedInEmpty).getSample();
        Assertions.assertTrue(sampleTreeSetComprator(mergeMainEmptyExpectedTree,mergemainEmptyTree));
        //update main fifosampler with sample elements
        for (SampleElement el : Sample1) {
            fifoSampler.update(el);
        }

        FiFoSampler other = new FiFoSampler(10);//other Fifo sampler
        //merge non-empty fifosampler with a empty other sample, should not effect it
        TreeSet<SampleElement> mergeEmptyExpectedTree = (TreeSet<SampleElement>)fifoSampler.getSample().clone();//
        TreeSet<SampleElement> mergeEmptyTree = fifoSampler.merge(other).getSample();
        Assertions.assertTrue(sampleTreeSetComprator(mergeEmptyExpectedTree,mergeEmptyTree));

    }

    @Test
    public void mergeTest() throws Exception {
        FiFoSampler fifoSampler = new FiFoSampler(10);//main fifo sampler other will be merged to.

        //update fifosampler with sample elements
        SampleElement[] Sample1= new SampleElement[]{new SampleElement("87",1), new SampleElement("198",5),
                new SampleElement("106",11),
                new SampleElement("34",12), new SampleElement("6", 17),
                new SampleElement("153", 20),new SampleElement("168",23),
                new SampleElement("90",24), new SampleElement("55", 28),new SampleElement("39",32)};

        for (SampleElement el : Sample1) {
            fifoSampler.update(el);
        }

        FiFoSampler other = new FiFoSampler(10);//other Fifo sampler
        //update other sampler with list of new samplers
        SampleElement[] Sample2= new SampleElement[]{new SampleElement("80",3), new SampleElement("98",6),
                new SampleElement("16",8),
                new SampleElement("34",13), new SampleElement("15", 19),
                new SampleElement("183", 22),new SampleElement("1668",25),
                new SampleElement("91",29), new SampleElement("55", 31),new SampleElement("59",32)};

        for (SampleElement el : Sample2) {
            other.update(el);
        }


        //create expected sample of merge result
        SampleElement[] mergeExpectedSample= new SampleElement[]{new SampleElement("153", 20),new SampleElement("183", 22),
                new SampleElement("168",23),new SampleElement("90",24),new SampleElement("1668",25),
                new SampleElement("55", 28),new SampleElement("91",29),new SampleElement("55", 31),
                new SampleElement("39",32),new SampleElement("59",32)};
        TreeSet<SampleElement> mergeExpectedTree = new TreeSet<>();
        for (SampleElement el : mergeExpectedSample) {
            mergeExpectedTree.add(el);
        }
        TreeSet<SampleElement> mergeTree = fifoSampler.merge(other).getSample();
        //compare actual and expected merge results
        Assertions.assertTrue(sampleTreeSetComprator(mergeTree,mergeExpectedTree));

    }

   public boolean sampleTreeSetComprator(TreeSet<SampleElement> tree1,TreeSet<SampleElement> tree2)
   {
       boolean Result=true;

       SampleElement element1, element2;
       if(tree1.size()!=tree2.size())
       {
           Result=false;
           System.out.println("size");
       }
       while(!tree1.isEmpty()&&!tree2.isEmpty()){

           element1= tree1.pollFirst();
           element2= tree2.pollFirst();
           if(((String)element1.getValue()).compareTo((String) element2.getValue()) != 0)
           {
               Result=false;
           }
       }
       return Result;
   }

}




