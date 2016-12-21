/* Michael Iuso
Hadoop MapReduce program to find the average video game 
rating of each publisher from an input file.
*/
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.InputStream;
public class Game_Ratings {

public static class mapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
     Text publisher = new Text();
     IntWritable gameRating = new IntWritable();
     String line = value.toString();
     Text gameName = new Text();
     StringTokenizer itr = new StringTokenizer(line,"\n");
     int i = 0;
     while (itr.hasMoreTokens()) {
        String results[] = line.split("\t");
        line = itr.nextToken();
        int size = results.length;
        if(size == 3){
                gameName.set(results[0]);
                publisher.set(results[1]);
                gameRating.set(new Integer(Integer.parseInt(results[2])));
                context.write(publisher,gameRating);
                }
        }
}
}


public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
private IntWritable result = new IntWritable();
public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
         int sum = 0;
         int gameCount = 0;
         int avgRating = 0;

         for (IntWritable value : values) {
               sum += value.get();
               gameCount = gameCount + 1;
        }
                avgRating = (int)(sum/gameCount);
                String[] tokens = values.toString().split("\t");
                result.set(avgRating);
                context.write(key, result);
        }	
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average rating");
    job.setJarByClass(GameRatings.class);
    job.setMapperClass(mapper.class);
    job.setCombinerClass(reducer.class);
    job.setReducerClass(reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
