package MapReduce.Aggrgation.Sum;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;

public class SumMapReducer {
	public static void main(String[]args) throws Exception {
		
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		Configuration conf = new Configuration();
		if(args.length!=2)
		{
			System.err.println("Usage:sumcalculator <input> <output>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf,"sum calculator");
		job.setJarByClass(SumMapReducer.class);
		job.setMapperClass(SumMapper.class);
		job.setReducerClass(SumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()),conf);
		fs.delete(outputPath,true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}


class SumMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	private final static Text sumKey = new Text("sum");
	
	public void map(LongWritable key, Text  value, Context context) throws IOException,InterruptedException
	{
		String line = value.toString().trim();
		if(!line.isEmpty())
		{
			try {
				int number = Integer.parseInt(line);
				context.write(sumKey, new IntWritable(number));
			}catch(NumberFormatException e) {
				
			}
		}
	}
}

class SumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	public void reduce(Text key,Iterable<IntWritable>values,Context context) 
			throws IOException,InterruptedException
	{
		int sum=0;
		for(IntWritable value:values)
		{
			sum+=value.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
