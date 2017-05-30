import java.io.IOException;

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




public class avg {
	public static class ma extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			int i=Integer.parseInt(arr[18]);
			
          
			
				context.write(new Text(arr[4]), new Text(arr[18]));
				
				}
		
	}
public static class re extends Reducer<Text,Text,Text,IntWritable>{
	
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
		int count=0;int sum=0,c=0;
		
		for(Text v:value)
		{
			int i=Integer.parseInt(v.toString());
			count++;
		sum+=i;	
		int ii=(sum*100)/count;
		if(i>ii)
		{
			 c = i;
		}
		}
		
		
		context.write(key, new IntWritable(c));
		

	}
} 
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
	Configuration obj=new Configuration();
	Job job=Job.getInstance(obj,"country");
	job.setJarByClass(avg.class);
	job.setMapperClass(ma.class);
job.setReducerClass(re.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
	//job.setNumReduceTasks(0);
	 job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    FileSystem.get(obj).delete(new Path(args[1]), true);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
     		
}
}
