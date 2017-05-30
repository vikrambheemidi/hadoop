import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class last5emp {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
		String str[]=value.toString().split(",");
		
				
		context.write(new Text("top 5"),new Text(str[33]+","+str[9]));
		}
	}
public static class Myreducer extends Reducer<Text,Text,Text,Text>
	{
	TreeMap tr=new TreeMap();
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		
		for(Text v:value)
		{
			String s[]=v.toString().split(",");
			int ss=Integer.parseInt(s[0]);
			int i=Integer.parseInt(s[1]);
			tr.put(ss,i);
			if(tr.size()>5)
			{
				tr.remove(tr.lastKey());
			}
			
		}
		context.write(key,new Text(tr.toString()));
		
	}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"max");
		job.setJarByClass(last5emp.class);
		job.setMapperClass(Mymapper.class);
		job.setReducerClass(Myreducer.class);
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
