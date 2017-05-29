import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class searchprgm {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String s=context.getConfiguration().get("work");
			String ss=value.toString().toLowerCase();
			String a=s.toLowerCase();
			String newText=s.toLowerCase();
			if(s!=null)
			{
				if(ss.contains(a))
				{
					context.write(new Text(ss),new IntWritable(1));
				}
			}
		
		}
	}
	public static class Myreducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
	{
		int sum=0;
	for(IntWritable v:value)
	{
		sum+=v.get();
	}
				
		
		context.write(key,new IntWritable(sum));
	}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		if(args.length>2)
		{
			conf.set("work", args[2]);
		}
		Job job=Job.getInstance(conf,"max");
		job.setJarByClass(searchprgm.class);
		job.setMapperClass(Mymapper.class);
		//job.setNumReduceTasks(0);
		job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
