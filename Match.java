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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Match {
	public static class ba extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			context.write(new Text(arr[7]), new IntWritable());
			

}}
		public static class at extends Reducer<Text,IntWritable,Text,IntWritable>{
			public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
				int count=0;
				for(IntWritable a:value){
					if(key.toString().contains("bat")){
						count++;
						context.write(key,new IntWritable(count));
						}}}
		}
		public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
			Configuration obj=new Configuration();
			Job job=Job.getInstance(obj,"country");
			job.setJarByClass(Match.class);
			job.setMapperClass(ba.class);
		job.setReducerClass(at.class);
			//job.setNumReduceTasks(1);
			 job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(IntWritable.class);
			    
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    FileSystem.get(obj).delete(new Path(args[1]), true);
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
		     		
			
		}
		}
