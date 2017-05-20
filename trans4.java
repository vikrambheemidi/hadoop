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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class trans4 {
public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>
{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String str[]=value.toString().split(",");
		if (Integer.parseInt(str[9])==0)
			context.write(new Text("total"),new Text(str[2]));		
	}
}
public static class Myreducer extends Reducer<Text,Text,Text,IntWritable>
{
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		int count=0;
     	for(Text x:value)
     	{
     		if(x.toString().contains("Widowed"))
     		{
     			count++;
     		}
     	}
     	context.write(key,new IntWritable(count));
	}
}
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"trans4");
	job.setJarByClass(trans4.class);
	job.setMapperClass(Mymapper.class);
	job.setReducerClass(Myreducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileSystem.get(conf).delete(new Path(args[1]),true);
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)?0:1);
}
}
