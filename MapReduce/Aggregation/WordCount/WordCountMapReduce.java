package Aggregation.WordCount;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;

import javax.xml.soap.Text;

public class WordCountMapReduce {
    public static void main(String[] args) throws Exception{
        String log4jConfPath = "log4jConfPath";
        PropertyConfigurator.configure(log4jConfPath);
        Configuration conf = new Configuration();

        if(args.length!=2){
            System.err.println("Usage: wordcount <in><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"word count");

        job.setJarByClass(WordCountMapReduce.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        Path outputPath = new Path(args[1]);
        FileSystem fs = new FileSystem.get(new URI(outputPath.toString()),conf);
        fs.delete(outputPath.true);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,outputPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text k2 = new Text();
    private IntWritable v2 = new IntWritable(1);

    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(v1.toString());
        while (st.hasMoreTokens()) {
            k2.set(st.nextToken());
            context.write(k2, v2);
        }
    }
}

class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable sum1 = new IntWritable();

    protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : v3) {
            sum += value.get();
        }
        sum1.set(sum);
        context.write(k3, sum1);
    }
}