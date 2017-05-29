

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class top5 {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
		String str[]=value.toString().split(",");
		int v=Integer.parseInt(str[4].toString());
		int w=Integer.parseInt(str[5].toString());
		int q=Integer.parseInt(str[9].toString());
		int x=Integer.parseInt(str[6].toString());
		//int y=Integer.parseInt(str[8].toString());
		if(x>1450)
			if(str[8].substring(4,9).contains("May 20"))
		if(q>60000 && q<120000)
			if(str[7].equals("Residential"))
				if(v==3)
					if(w==2)
				
		context.write(new Text(str[1]),new Text(str[7]+" "+q));
		}
	}
public static class Myreducer extends Reducer<Text,Text,Text,Text>
	{
	TreeMap tr=new TreeMap();
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		int max=0;
		for(Text v:value)
		{
			String[] s1=v.toString().split(",");
			int i=Integer.parseInt(s1[1].toString());
			if(i>max)
			{
				max=i;
			}
			tr.put(v,max);
			if(tr.size()>5)
			{
				tr.remove(tr.firstKey());
			}
			
		}
		String[] s1 = null;
		context.write(key,new Text(tr.toString()+" "+s1[0]));
	}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"max");
		job.setJarByClass(top5.class);
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
