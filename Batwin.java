import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;

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




public class Batwin {
	public static class ma extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			int a=Integer.parseInt(arr[1]);

			if(arr[6].equals(arr[10])){
				if(arr[7].contains("bat")){
			
				context.write(new Text(arr[14]), new Text(arr[14]));
				
				}}
		}
	}
public static class re extends Reducer<Text,Text,Text,Text>{
	HashSet hs=new HashSet();
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
		int count=0;int max=0;
		
		for(Text values:value)
			count++;
			
			if(count>max){
				count=max;
			}
		
			if(hs.size()>3){
				hs.add(max);
			}
	    	
	    	
		
			
		
		context.write(key, new Text(hs.toString()));
		

	}
} 
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
	Configuration obj=new Configuration();
	Job job=Job.getInstance(obj,"country");
	job.setJarByClass(Batwin.class);
	job.setMapperClass(ma.class);
job.setReducerClass(re.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
	//job.setNumReduceTasks(1);
	 job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    FileSystem.get(obj).delete(new Path(args[1]), true);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
     		
}}

