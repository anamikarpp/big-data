import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;

public class Anagram {
	
	public static void main(String[] args) throws Exception {
		
		String log4jConfPath = "log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		Configuration conf = new Configuration();
		
		if(args.length!=2)
		{
			System.err.println("Usage: G_AnagramGrouper <input> <output>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf,"G_Anagram");
		job.setJarByClass(Anagram.class);
		job.setMapperClass(AnagramMapper.class);
		job.setReducerClass(AnagramReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()),conf);
		fs.delete(outputPath,true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}

class AnagramMapper extends Mapper<LongWritable,Text,Text,Text>{
	private Text sortedKey = new Text();
	private Text originalWord = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String word = value.toString().trim().toLowerCase();
		if(!word.isEmpty())
		{
			char[] chars= word.toCharArray();
			Arrays.sort(chars);
			sortedKey.set(new String(chars));
			originalWord.set(word);
			context.write(sortedKey,originalWord);
		}
	}
}

class AnagramReducer extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key,Iterable<Text>values,Context context) throws IOException,InterruptedException
	{
		StringBuilder grouped = new StringBuilder(); 
		int count=0;
		for(Text word:values)
		{
			grouped.append(word.toString()).append(" ");
			count++;
		}
		if(count>2) {
		context.write(key, new Text(grouped.toString().trim()));
		}
	}
}
