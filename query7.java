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

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//","CASE_STATUS","EMPLOYER_NAME","SOC_NAME","JOB_TITLE","FULL_TIME_POSITION","PREVAILING_WAGE","YEAR","WORKSITE","longitude","latitude"
//Create a bar graph to depict the number of applications for each year
//"2","CERTIFIED-WITHDRAWN","GOODMAN NETWORKS, INC.","CHIEF EXECUTIVES","CHIEF OPERATING OFFICER","Y",242674,2016,"PLANO, TEXAS",-96.6988856,33.0198431
public class query7 {
    public static class mapperclass extends Mapper<LongWritable,Text,Text,Text>
    {

        public  void map(LongWritable key ,Text value,Context context)throws IOException,InterruptedException
        {
        	String str[]=value.toString().split("\t");

            
            
            
                context.write(new Text(str[7]),new Text(str[1]));                                                       
            
        }
    }
    public static class reducerclass extends Reducer<Text,Text,Text,IntWritable>
    {
        
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
            int count=0;
            for(Text i:values)
            {
                count++;
            }
            context.write(key, new IntWritable(count));
        }
    }
    public static void main(String[] args)throws Exception
    {
    Configuration config =new Configuration();
    Job job =Job.getInstance(config,"count");
       job.setJarByClass(query7.class);
       job.setMapperClass(mapperclass.class);
       job.setReducerClass(reducerclass.class);
       
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileSystem.get(config).delete(new Path(args[1]),true);
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}