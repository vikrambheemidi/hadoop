import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class gendliteracy {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			
			String str[]=value.toString().split(",");
			if(!str[1].contains("Children"))
             
			context.write(new Text("%"),new Text(str[1]+","+str[3]));
		}
		
	}
	public static class Myreducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
		{
			float e=0,x3=0,y1=0;
			int counts = 0;;
			float cnt=0;String edu="";
			float count=0;
			String z="";String gender="";
			int s=0;
			for(Text v:value)
			{
				String st[]=v.toString().split(",");
				edu=st[0];
				gender=st[1];
				
				counts++;
				
				 if(gender.toString().equals("Female"))
				{
					count++;
				}
				else if(gender.toString().equals("Male"))
				{
					cnt++;
				}
				 e=(count*100)/counts;
				 x3=(cnt*100)/counts;
				 z=e+" "+x3;
				 
			}
			 
			
			context.write(key,new Text(z));
		}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"max");
		job.setJarByClass(gendliteracy.class);
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
