import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class query4 extends Configured implements Tool
{
	public static class myclass extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String parts[] = value.toString().split("\t");
			
			context.write(new Text(parts[2]), value);
		}
	}
	public static class mypart extends Partitioner<Text,Text>
	{
		public int getPartition(Text key,Text value,int numReduceTasks)
		{
			String parts[] = value.toString().split("\t");
			if(numReduceTasks == 0)
			{
				return 0;
			}
			if(parts[7].equals("2011"))
			{
				return 0;
			}
			if(parts[7].equals("2012"))
			{
				return 1;
			}
			if(parts[7].equals("2013"))
			{
				return 2;
			}
			if(parts[7].equals("2014"))
			{
				return 3;
			}
			if(parts[7].equals("2015"))
			{
				return 4;
			}
			else
				return 5;
		}
	}
	public static class myreducer extends Reducer<Text,Text,NullWritable,Text>
	{

		private TreeMap<Integer,Text>  val1 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  val2 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  val3 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  val4 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  val5 = new TreeMap<Integer,Text>();
		private TreeMap<Integer,Text>  val6 = new TreeMap<Integer,Text>();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			int count=0;
			String year = "";
			String myval = "";
			
			for(Text t:values)
			{
				String parts[] = t.toString().split("\t");
				count++;
				year = parts[7];				
			}
			myval = key.toString();
			myval = myval+','+year+','+count;
			if(year.equals("2011"))
			{
				val1.put(count, new Text(myval));
				if(val1.size()>5)
				{
					val1.remove(val1.firstKey());
				}
			}
			if(year.equals("2012"))
			{
				val2.put(count, new Text(myval));
				if(val2.size()>5)
				{
					val2.remove(val2.firstKey());
				}
			}
			if(year.equals("2013"))
			{
				val3.put(count, new Text(myval));
				if(val3.size()>5)
				{
					val3.remove(val3.firstKey());
				}
			}
			if(year.equals("2014"))
			{
				val4.put(count, new Text(myval));
				if(val4.size()>5)
				{
					val4.remove(val4.firstKey());
				}
			}
			if(year.equals("2015"))
			{
				val5.put(count, new Text(myval));
				if(val5.size()>5)
				{
					val5.remove(val5.firstKey());
				}
			}
			if(year.equals("2016"))
			{
				val6.put(count, new Text(myval));
				if(val6.size()>5)
				{
					val6.remove(val6.firstKey());
				}
			}
		}
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			for(Text t:val1.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:val2.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:val3.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:val4.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:val5.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
			for(Text t:val6.descendingMap().values())
			{
				context.write(NullWritable.get(),t);
			}
		}
	}
	public int run(String []args) throws Exception
	{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job name");
		job.setJarByClass(query4.class);
		job.setPartitionerClass(mypart.class);
		job.setMapperClass(myclass.class);
		job.setReducerClass(myreducer.class);
		job.setNumReduceTasks(6);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	public static void main(String []ar) throws Exception
	{
		ToolRunner.run(new Configuration(), new query4(), ar);
	}

	
}