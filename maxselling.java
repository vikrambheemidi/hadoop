import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class maxselling {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
		String str[]=value.toString().split(",");
		//int a=Integer.parseInt(str[4]);
		//int b=Integer.parseInt(str[2]);
		context.write(new Text(str[3]),new Text(str[2]+" "+str[4]));
		}
	}
	public static class part extends Partitioner<Text,Text>
	{
		public int getPartition(Text key,Text value,int partition)
		{
			String str[]=value.toString().split(",");
			int a=Integer.parseInt(str[0]);
			if(a<20)
			{
				return 0;
			}
			if(a>20 && a<30)
			{
				return 1;
			}
			else if(a>30)
			{
				return 2;
			}
			else
			{
				return partition;
			}
				
		}
	}
	
	
	public static class Myreducer extends Reducer<Text,Text,Text,Text>
	{
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
	{
		int sum=0,count=0,x=0;
		String s="",y="";
		for(IntWritable v:value)
		{
			String s1[]=v.toString().split(",");
			int a=Integer.parseInt(s1[1]);
			x=Integer.parseInt(s1[1].toString());
			if(a>sum)
			{
				sum=a;
			}
			
			
		}
		String
		 
		context1.write(key,new Text(y));
	}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"max");
		job.setJarByClass(maxselling.class);
		job.setMapperClass(Mymapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(part.class);
		job.setReducerClass(Myreducer.class);
		job.setNumReduceTasks(3);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
