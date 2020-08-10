package Benchmark;

import Synopsis.Histograms.EquiWidthHistogram;
import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.ReservoirSampler;
import Synopsis.Sketches.CountMinSketch;
import Synopsis.StratifiedSynopsis;
import Synopsis.Wavelets.WaveletSynopsis;
import java.io.*;
import java.util.zip.GZIPInputStream;

/*java -jar target/EDADS-0.1.jar CountMinSketch /home/zahra/EDADS/scotty-window-processor/EDADS/Data/uniformTimestamped.gz /home/zahra/EDADS/results*/

public class SingleNode {
    private static String synType;

    public static void main(String[] args) {
        String dataFilePath = args[1];//"/home/zahra/EDADS/scotty-window-processor/EDADS/Data/uniformTimestamped.gz";//
        synType = args[0];
        //System.out.println(this.getClass().getResourceAsStream("abc.txt"));
        String outputPath = args[2];//"/home/zahra/EDADS/results";//
        //synType = "wavelet";
        try {
            PrintWriter out = new PrintWriter(new FileOutputStream(new File(outputPath + "/single-node.txt"), true), true);
            //PrintWriter out = new PrintWriter(new FileOutputStream(new File(outputPath + "/single-node.txt"), true), true);
            String line;
            String result = "Throughput=";
            String numRecords = "";
            MergeableSynopsis synopsis;
           if (synType.equals("wavelet")) {
                 StratifiedSynopsis wavesynopsis = new WaveletSynopsis(10000);
                for (int i = 0; i < 10; i++) {
                    GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
                    //GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(new File(getClass().getResource(dataFilePath).toURI())));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));
                    long number = 0;
                    long startTime = System.currentTimeMillis();
                    System.out.println("iteration: " + i);
                    while (reader.ready() && (line = reader.readLine()) != null) {

                        String[] tokens = line.split(",");

                        ((WaveletSynopsis<Double>) wavesynopsis).update(Double.parseDouble(tokens[0]));

                        number++;
                    }

                    long endTime = System.currentTimeMillis();
                    float timeElapsed = (endTime - startTime) / 1000f;
                //System.out.println(startTime);
                // System.out.println(endTime);
                // System.out.println(timeElapsed);
                // System.out.println("Number of records: "+number);
                //System.out.println("Throughput: "+throughput);
                //System.out.println("---------------------");

                    float throughput = (number / timeElapsed);
                    result += throughput + ",";
                    numRecords += (number + ",");


                }
                //System.out.println(synopsis.toString());

           }

           else {
                synopsis = getSynopsis(synType);

                for (int i = 0; i < 10; i++) {
                    GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));
                    long number = 0;
                    long startTime = System.currentTimeMillis();
                    System.out.println("iteration: " + i);
                    while (reader.ready() && (line = reader.readLine()) != null) {

                        String[] tokens = line.split(",");
                        if (synType.equals("EquiWidthHistogram")) {
                            synopsis.update(Double.parseDouble(tokens[0]));
                        } else {
                            synopsis.update(tokens[0]);
                        }


                        number++;
                    }

                    long endTime = System.currentTimeMillis();
                    float timeElapsed = (endTime - startTime) / 1000f;
               /* System.out.println(startTime);
                System.out.println(endTime);
                System.out.println(timeElapsed);
                System.out.println("Number of records: "+number);
                //System.out.println("Throughput: "+throughput);
                //System.out.println("---------------------");*/

                    float throughput = (number / timeElapsed);
                    result += throughput + ",";
                    numRecords += (number + ",");


                }
                //System.out.println(synopsis.toString());

           }
            System.out.println(result);
            System.out.println("Number of records: " + numRecords);
            out.append("\nSource: Uniform, Synopsis: " + synType);
            out.append("\n" + result);
            out.append("\nNumber of records: " + numRecords);
            out.append("\n--------------------------------------------------------------------");
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

        private static MergeableSynopsis getSynopsis (String syn){
            if (syn.equals("CountMinSketch")) {
                return new CountMinSketch(65536, 5, 7L);
            } else if (syn.equals("ReservoirSampler")) {
                return new ReservoirSampler(10000);
            }
            else if (syn.equals("EquiWidthHistogram")) {
                return new EquiWidthHistogram(0.0, 101.0, 10);
            }


            throw new IllegalArgumentException(syn + " is not a valid synopsis for benchmarking");
        }
    }
