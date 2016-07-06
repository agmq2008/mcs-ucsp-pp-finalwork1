import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Math;
import java.lang.Integer;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FloydWarshall_MP {

  private static int TAM_N = 100;

  public static class FloydWarshallMapper
       extends Mapper<Object, Text, MyKeyMap, MyValMap>{

    private MyValMap myValMap;
    private MyKeyMap myKeyMap;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(value.toString().length() != 0){
          StringTokenizer itr = new StringTokenizer(value.toString());
          int[] arrayInt = new int[5];
          int m = 0;
          // 0 -> i, 1 -> j, 2 -> k, 3 -> dist[i][j], 4->pre[i][j]

          while (itr.hasMoreTokens()) {          
              arrayInt[m] = Integer.parseInt(itr.nextToken());
              m++;
          }
          // System.out.println("mapper ==> " + arrayInt[0] + " " + arrayInt[1]);        
          if(arrayInt[0] == arrayInt[2] || arrayInt[1] == arrayInt[2]){
              for(m = 0 ; m < TAM_N; m++){              
                  myValMap = new MyValMap(arrayInt[0], arrayInt[1], arrayInt[2], arrayInt[3], arrayInt[4]);
                  if(arrayInt[1] == arrayInt[2]){
                      myKeyMap = new MyKeyMap(arrayInt[0], m);                  
                      // System.out.println("=> " + myKeyMap.toString() + " || " + myValMap.toString()) ;
                      context.write(myKeyMap, myValMap);
                  }    
                  if(arrayInt[0] == arrayInt[2]){
                      myKeyMap = new MyKeyMap(m, arrayInt[1]);
                      // System.out.println("=> " + myKeyMap.toString() + " || " + myValMap.toString()) ;
                      context.write(myKeyMap, myValMap);                    
                  }                    
              }
          }else{
              myKeyMap = new MyKeyMap(arrayInt[0], arrayInt[1]);
              myValMap = new MyValMap(arrayInt[0], arrayInt[1], arrayInt[2], arrayInt[3], arrayInt[4]);
              context.write(myKeyMap, myValMap);
              // System.out.println("=> " + myKeyMap.toString() + " || " + myValMap.toString()) ;
          }
          // System.out.println("===================");
        }
    }
  }

  public static class FloydWarshallReducer extends Reducer<MyKeyMap,MyValMap,Text,Text> {
    
    private Text keyText = new Text();
    private Text valText = new Text();
    private MyValMap myValMap;
    private MyKeyMap myKeyMap;

    public void reduce(MyKeyMap key, Iterable<MyValMap> values, Context context ) throws IOException, InterruptedException {

        int i = key.indI.get();
        int j = key.indJ.get();
        int k = 0, distIK = 0, distIJ = 0, distKJ = 0, prefIJ = 0, prefKJ = 0; 
        int tempI = 0, tempJ = 0;

        List<MyValMap> listValMap = new ArrayList<MyValMap>();
        // System.out.print(i + " " + j + " => ");
        for(MyValMap val : values){                        
            listValMap.add(new MyValMap(val.indI.get(), val.indJ.get(), val.kIter.get(), val.distIJ.get(), val.prefIJ.get()));
        }
        // for(int l = 0; l < listValMap.size(); l++)
        //   System.out.print(listValMap.get(l).toString() + " || ");

        // System.out.println();
        if(listValMap.size() == 2){
            // significa que i == k o j == k
            // entonces no existe modificacion para dist[i][j]
            for(MyValMap val : listValMap){
                tempI = val.indI.get();
                tempJ = val.indJ.get();                
                k = val.kIter.get();
                if(tempI == i && tempJ == j){
                  distIJ = val.distIJ.get();
                  distKJ = val.distIJ.get();
                  distIK = val.distIJ.get();
                  prefIJ = val.prefIJ.get();    
                }    
            }
        }else{ 
            // significa que i != k && j != k            
            for(MyValMap val : listValMap){
                tempI = val.indI.get();
                tempJ = val.indJ.get();                
                k = val.kIter.get();
                if(tempI == i && tempJ == j){
                    distIJ = val.distIJ.get();
                    prefIJ = val.prefIJ.get();    
                }else if(tempI == i && tempJ == k){
                    distIK = val.distIJ.get();                    
                }else if(tempI == k && tempJ == j){
                    distKJ = val.distIJ.get();
                    prefKJ = val.prefIJ.get();     
                }   
            }
        }
        prefIJ = distIJ <= distIK + distKJ ? prefIJ : prefKJ;
        distIJ = Math.min(distIJ, distIK + distKJ);
        String keyStr = i + " "  + j; 
        String valStr = (k + 1) + " " + distIJ + " " + prefIJ;

        keyText.set(keyStr);
        valText.set(valStr);

        context.write(keyText, valText);
    }
  }

  public static void main(String[] args) throws Exception {    
      long time = new Date().getTime();
      Configuration conf = new Configuration();
      //TAM_N = Integer.parseInt(args[0]);
      String inStrPath = args[1];
      String outStrPath = args[2];
      String tempStrPath  = "";


      FileSystem fs = FileSystem.get(conf);
      for(int k = 0; k < TAM_N; k++){          
          Job job = Job.getInstance(conf, "FloydWarshall_MP");
          job.setJarByClass(FloydWarshall_MP.class);
          job.setMapperClass(FloydWarshallMapper.class);        
          job.setReducerClass(FloydWarshallReducer.class);
          job.setMapOutputKeyClass(MyKeyMap.class);
          job.setMapOutputValueClass(MyValMap.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
          FileInputFormat.addInputPath(job, new Path(inStrPath));
          FileOutputFormat.setOutputPath(job, new Path(outStrPath));
          job.waitForCompletion(true);

          if(fs.exists(new Path(inStrPath)))
              fs.delete(new Path(inStrPath), true);
          if(fs.exists(new Path(outStrPath))) 
              fs.delete(new Path(outStrPath + "/_SUCCESS"), true);

          tempStrPath = inStrPath;
          inStrPath = outStrPath;          
          outStrPath = tempStrPath;
      } 
      time = new Date().getTime() - time;
      double timeFinal = time / 1000;
      System.out.println("Total time " + timeFinal + " seconds.");
  }
}