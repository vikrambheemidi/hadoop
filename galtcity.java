import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class galtcity {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
		String str[]=value.toString().split(",");
		String s=str[7]+" "+str[9];
		int q=Integer.parseInt(str[9].toString());
		if(q>10000)
			if(str[7].equals("Condo"))
				
		context.write(new Text(str[1]),new Text(s));
		}
	}
/*	public static class Myreducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		double sum=0;
		for(Text v:value)
		{
			sum+=Double.parseDouble(v.toString());
			
		}
		context.write(key,new DoubleWritable(sum));
	}
	}*/
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"max");
		job.setJarByClass(galtcity.class);
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
