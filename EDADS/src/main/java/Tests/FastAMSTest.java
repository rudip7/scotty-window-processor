package Tests;
import Synopsis.Sketches.DDSketch;
import Synopsis.Sketches.FastAMS;
import Synopsis.Sketches.CountMinSketch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * @author Zahra Salmani
 */


public class FastAMSTest {

    @Test
    public void updateTest(){
        int width=200;
        int height=10;
        FastAMS fastAMS=updateFromFile(new FastAMS(width,height,345345L),"data/dataset.csv");

        int [][] fastAMSArray =fastAMS.getArray();
        int [] rowSumArray= new int [height];
        int rowSum=0;
        for(int i=0; i< height;i++){
            rowSum=0;
            for (int j=0; j<width;j++){
                rowSum+=Math.abs(fastAMSArray[i][j]);
            }
            rowSumArray[i]=rowSum;
        }

        for (int element :rowSumArray){
            System.out.println(element);
            //Assertions.assertTrue(element==4000);
        }



    }

    private FastAMS updateFromFile(FastAMS fastAMS, String fileName){
        //String fileName= file;
        File file= new File(fileName);
        // this gives you a 2-dimensional array of strings
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                //line=line.substring(0, line.length() - 1);
                fastAMS.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return fastAMS;
    }

}
