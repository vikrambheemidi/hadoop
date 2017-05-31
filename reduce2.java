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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class reduce2 {
	public static class Mymapper1 extends Mapper<LongWritable,Text,IntWritable,Text>{
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
        {
            String str[]=value.toString().split(",");
            
				context.write(new IntWritable(Integer.parseInt(str[0])),new Text(str[1]+"\ta") );
			
           
        }
    }
    public static class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
        {
            String arr[]=value.toString().split(",");
                    
						context.write(new IntWritable(Integer.parseInt(arr[0])),new Text(arr[1]+"\tb"));
        }
    }

    public static class re extends Reducer<IntWritable,Text,IntWritable,Text>{
        public void reduce(IntWritable key,Iterable<Text> value,Context context) throws IOException, InterruptedException  
        {
        
           int name=0,sum=0,j=0,sum1=0;
           String result="";
            for(Text v:value){
                String ss[]=v.toString().split("\t");

                if(ss[1].equals("a")){
           
                    name=Integer.parseInt(ss[0]);
                   sum+=name;
                }
           
                else    if(ss[1].equals("b")){
                	j=Integer.parseInt(ss[0]);
                	
                	sum1+=j;
                	
                	
            // String x=String.valueOf(sum);
               
            }
                result =sum1+" "+sum;
            
        }
            
				
				context.write(key, new Text(result));
        	 
        }
        public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException  
        {
            Configuration obj=new Configuration();
            Job job = null;
        
				job = Job.getInstance(obj,"country");
			
            job.setJarByClass(reduce2.class);
           
        job.setReducerClass(re.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
            
             job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(IntWritable.class);
               MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class , Mymapper1.class);
               MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MyMapper.class);
              
                FileOutputFormat.setOutputPath(job, new Path(args[2]));
                
					FileSystem.get(obj).delete(new Path(args[2]), true);
				
					
					
					System.exit(job.waitForCompletion(true) ? 0 : 1);
				
                    
    }
}
}
