import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;




public class Texttonull {
	public static class Myclass extends Mapper<LongWritable, Text,LongWritable, Text> {
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String s=value.toString();
		String ss[]=s.split(",");
		int i=Integer.parseInt(ss[0]);
		context.write(new LongWritable(i),new Text(ss[1]));
	}
}
public static void main(String[] args) throws Exception {
Configuration c = new Configuration();
//c.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

Job job = Job.getInstance(c,
		"break string into 2 parts i.e key and value ");
job.setJarByClass(Texttonull.class);
job.setMapperClass(Myclass.class);

job.setNumReduceTasks(0);
job.setOutputKeyClass(LongWritable.class);
job.setOutputValueClass(Text.class);

//job.setInputFormatClass(KeyValueTextInputFormat.class);
//job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(NullOutputFormat.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

FileSystem.get(c).delete(new Path(args[1]), true);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
