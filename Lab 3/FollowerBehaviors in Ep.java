import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Following {

 public static class FollowingMap extends Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token1 = tokenizer.nextToken();
            String token2 = tokenizer.nextToken();
            String follow = token1 + " " + token2;
            context.write(new IntWritable(Integer.parseInt(token1)), new Text(follow));
            context.write(new IntWritable(Integer.parseInt(token2)), new Text(follow));
        }
    }
}

 public static class FollowingReduce extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
        String following = ",[";
        String follower = ",[";

        String line = "";
        for (Text val : values) {
            line += val.toString();
            line += " ";
        }
        boolean followingIsNull = true;
        boolean followerIsNull = true;

        StringTokenizer tokenizer = new StringTokenizer(line);
        while(tokenizer.hasMoreTokens()){
            String token1 = tokenizer.nextToken();
            String token2 = tokenizer.nextToken();
            int int_key = key.get();
            String str_key = Integer.toString(int_key);
            if ( str_key.equals(token1) ){
                following += token2;
                following += ",";
                followingIsNull = false;
            }
            else{
                follower += token1;
                follower += ",";
                followerIsNull = false;
            }
        }
        following = following.substring(0, following.length()-1);
        follower = follower.substring(0, follower.length()-1);

        if(followingIsNull == false){
            following += "] --> following list";
        }
        else{
            following += "[ none ] --> following list";
        }

        if(followerIsNull == false){
            follower += "] --> follower list";
        }
        else{
            follower += "[ none ] --> follower list";
        }

        context.write(key, new Text(following));
        context.write(key, new Text(follower));
    }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "Following Behaviors in Ep.txt");

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(FollowingMap.class);
    job.setReducerClass(FollowingReduce.class);

    job.setJarByClass(Following.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

 }

}