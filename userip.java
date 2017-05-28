import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileEncryptionInfo;
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


public class userip {
public static class Mymapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
	public void map(LongWritable key,Text value,Context context)
	{
		String str[]=value.toString().split(",");
		String yr=context.getConfiguration().get("work");
		int i=Integer.parseInt(yr.toString());
		int ii=Integer.parseInt(str[0].toString());
		try {
			context.write(new Text("count"), new IntWritable(ii+i));
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	}	
}
public static class Myreducer extends Reducer<Text,IntWritable,Text,IntWritable>
{
	public void reduce(Text key,Iterable<IntWritable> value,Context context)
	{int count=0;
		for(IntWritable v:value)
		{
			
			int x=Integer.parseInt(v.toString());
			if(x>18)
			{
				count++;
			}
		}
		try {
			context.write(key,new IntWritable(count));
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	}
}
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
	Scanner s=new Scanner(System.in);
	System.out.println("no. of years");
	String ss=s.next();
	conf.set("work",ss);
	Job job=Job.getInstance(conf,"work1");
	job.setJarByClass(userip.class);
	job.setMapperClass(Mymapper.class);
	job.setReducerClass(Myreducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileSystem.get(conf).delete(new Path(args[1]), true);
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)?0:1);
}
}
