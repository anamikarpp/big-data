import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;

public class AverageWithCombiner {
	public static void main(String[]args) throws Exception {
	String log4jConfPath = "log4j.properties";
	PropertyConfigurator.configure(log4jConfPath);
	Configuration conf = new Configuration();
	if(args.length!=2)
	{
		System.err.println("Usage:averageCalculator <input> <output>");
		System.exit(2);
	}
	
	Job job = Job.getInstance(conf,"average calculator");
	job.setJarByClass(AverageWithCombiner.class);
	job.setMapperClass(AverageMapper.class);
	job.setCombinerClass(AverageCombiner.class);
	job.setReducerClass(AverageReducer.class);
	
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);
	
	Path outputPath = new Path(args[1]);
	FileSystem fs = FileSystem.get(new URI(outputPath.toString()),conf);
	fs.delete(outputPath,true);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, outputPath);
	
	System.exit(job.waitForCompletion(true)?0:1);
	}
}

class AverageMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
	private final static Text sumKey = new Text("Average");
	static IntWritable one = new IntWritable(1);
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		String line = value.toString().trim();
		if(!line.isEmpty())
		{
			try {
				int number = Integer.parseInt(line);
				context.write(new IntWritable(number), one);
			}catch(NumberFormatException e) {
				
			}
		}
	}
}
class AverageCombiner extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
	{
		int sum=0;
		for(IntWritable value:values)
		{
			sum+=value.get();
		}
		context.write(key,new IntWritable(sum));
		
	}
	
}
class AverageReducer extends Reducer<IntWritable,IntWritable,Text,IntWritable>{
	private int totalSum=0;
	private int totalCount=0;
	
	public void reduce(IntWritable key,Iterable<IntWritable>values,Context context) 
			throws IOException,InterruptedException
	{
		int count=0;
		for(IntWritable value:values)
		{
			count+=value.get();
		}
		totalSum+=key.get()*count;
		totalCount+=count;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		context.write(new Text("Average"),new IntWritable(totalSum/totalCount));
	}
}
