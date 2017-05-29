

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class cheapestcondo {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
		String str[]=value.toString().split(",");
		String s=str[1]+" "+str[0];
		
		if(str[3].equals("CA"))
			if(str[7].equals("Condo"))
				
		context.write(new Text(str[3]),new Text(s+" "+str[9]));
		}
	}
	public static class Myreducer extends Reducer<Text,Text,Text,Text>
	{
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		int min=0;
		for(Text v:value)
		{
			String[] s1=v.toString().split(",");
			int i=Integer.parseInt(s1[1].toString());
			if(i>min)
			{
				min=i;
			}
			
		}
		String[] s1 = null;
		String ss=s1[0]+" "+min;
		context.write(key,new Text(ss));
	}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"max");
		job.setJarByClass(cheapestcondo.class);
		job.setMapperClass(Mymapper.class);
		//job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
