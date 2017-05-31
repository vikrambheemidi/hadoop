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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class sample1 {
	public static class Mymapper1 extends Mapper<LongWritable,Text,IntWritable,Text>{
        public void map(LongWritable key,Text value,Context context) 
        {
            String str[]=value.toString().split(",");
            try {
				context.write(new IntWritable(Integer.parseInt(str[0])),new Text(str[1]+"\ta") );
			} catch (NumberFormatException e) {
				
				e.printStackTrace();
			} catch (IOException e) {
				
				e.printStackTrace();
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
           
        }
    }
    public static class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        public void map(LongWritable key,Text value,Context context)
        {
            String arr[]=value.toString().split(",");
                    try {
						context.write(new IntWritable(Integer.parseInt(arr[0])),new Text(arr[1]+","+arr[3]+"\tb"));
					} catch (NumberFormatException e) {
						
						e.printStackTrace();
					} catch (IOException e) {
						
						e.printStackTrace();
					} catch (InterruptedException e) {
						
						e.printStackTrace();
					}
        }
    }

    public static class re extends Reducer<IntWritable,Text,IntWritable,Text>{
        public void reduce(IntWritable key,Iterable<Text> value,Context context)  
        {
        
            String name="";String str1="";
                    String str="";int result=0;
                    int marks=0,mks=0;
                    String result1="";
            for(Text v:value){
                String ss[]=v.toString().split("\t");

                if(ss[1].equals("a")){
           
                   marks=Integer.parseInt(ss[0]);                   
                }
           
                else    if(ss[1].equals("b")){
                	String vr[]=ss[0].toString().split(","); 
               
                str=vr[0];
                mks=Integer.parseInt(vr[1].toString());
                result=marks%mks;
                result1 =result+" "+str;
            }
              
            
        }
            try {
				context.write(key, new Text(result1));
			} catch (IOException e) {
				
				e.printStackTrace();
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
        	 
        }
        public static void main(String args[])  
        {
            Configuration obj=new Configuration();
            Job job = null;
			try {
				job = Job.getInstance(obj,"country");
			} catch (IOException e2) {
				
				e2.printStackTrace();
			}
            job.setJarByClass(sample1.class);
           
        job.setReducerClass(re.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
            
             job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);
               MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class , Mymapper1.class);
               MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MyMapper.class);
              
                FileOutputFormat.setOutputPath(job, new Path(args[2]));
                try {
					FileSystem.get(obj).delete(new Path(args[2]), true);
				} catch (IllegalArgumentException e1) {
					
					e1.printStackTrace();
				} catch (IOException e1) {
					
					e1.printStackTrace();
				}
                try {
					System.exit(job.waitForCompletion(true) ? 0 : 1);
				} catch (ClassNotFoundException e) {
					
					e.printStackTrace();
				} catch (IOException e) {
					
					e.printStackTrace();
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				}
                    
    }
	
    }
}
