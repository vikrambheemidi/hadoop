import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class query3 {
    
	public static class mapperclass extends Mapper<LongWritable,Text,Text,Text>
    {

        public  void map(LongWritable key ,Text value,Context context)throws IOException,InterruptedException
        {
        	String str[]=value.toString().split("\t");

            String detect = str[4];
            
            if(detect.matches("DATA SCIENTIST"))
            {
                context.write(new Text(str[3]),new Text(detect));                                                       
            }
        }
    }
    public static class reducerclass extends Reducer<Text,Text,NullWritable,Text>
    {
    	private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();   
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
            int count=0;
            for(Text i:values)
            {
                count++;
            }
            
            String myValue = key.toString();
            myValue = myValue+","+count;
            repToRecordMap.put(new Long(count), new Text(myValue));
			if (repToRecordMap.size() > 1) 
			{
				repToRecordMap.remove(repToRecordMap.firstKey());
            }
    }
        protected void cleanup(Context context) throws IOException,
		InterruptedException {
	for (Text t : repToRecordMap.values()) {
		context.write(NullWritable.get(), t);
	}
}
}
    public static void main(String[] args)throws Exception
    {
    Configuration config =new Configuration();
    Job job =Job.getInstance(config,"count");
       job.setJarByClass(query3.class);
       job.setMapperClass(mapperclass.class);
       job.setReducerClass(reducerclass.class);
       
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileSystem.get(config).delete(new Path(args[1]),true);
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}