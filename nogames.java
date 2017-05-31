import java.io.IOException;
import java.util.TreeSet;



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


public class nogames {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	      {	    	  
	         String s[]=value.toString().split("	");
	            
	            context.write(new Text("count"),new Text(s[5]));
	           
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	   {
		    TreeSet tr=new TreeSet();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      
				
		         for (Text val : values)
		         {       
		        	 tr.add(val.toString());
		        	 
		         }
		         
		      		      
		      context.write(key, new IntWritable(tr.size()));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(nogames.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		   job.setReducerClass(ReduceClass.class);
		   job.setMapOutputKeyClass(Text.class);
		   job.setMapOutputValueClass(Text.class);
		    //job.setNumReduceTasks(2);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(conf).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
