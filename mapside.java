import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class mapside {
public static class Myclass extends Mapper<LongWritable,Text,Text,Text>
{
	private Map<String,String> abmap=new HashMap<String,String>();
	private Map<String,String> abmap1=new HashMap<String,String>();
	
	private Text outputkey=new Text();
	private Text outputvalue=new Text();
	
	protected void setup(Context context) throws IOException
	{
		URI[] files=context.getCacheFiles();
		Path p=new Path(files[0]);
		Path p1=new Path(files[1]);
		
		if(p.getName().equals("salary.txt"))
		{
			BufferedReader reader=new BufferedReader(new FileReader(p.toString()));
			String line=reader.readLine();
			while(line !=null)
			{
				String tokens[]=line.split(",");
				String empid=tokens[0];
				String empsal=tokens[1];
				abmap.put(empid,empsal);
				line=reader.readLine();
			}
		}
		if(abmap.isEmpty())
		{
			throw new IOException("MyError:Unable to load data");
		}
		if(p1.getName().equals("desig.txt"))
		{
			BufferedReader reader1=new BufferedReader(new FileReader(p1.toString()));
			String line1=reader1.readLine();
			while(line1 !=null)
			{
				String tokens[]=line1.split(",");
				String empid=tokens[0];
				String empdesig=tokens[1];
				abmap1.put(empid,empdesig);
				line1=reader1.readLine();
			}
		}
		if(abmap1.isEmpty())
		{
			throw new IOException("MyError:Unable to load data");
		}
	}
	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String row=value.toString();
		String rows[]=row.split(",");
		String empid=rows[0];
		String salary=abmap.get(empid);
		String desig=abmap1.get(empid);
		String myoutput=salary+","+desig;
		outputkey.set(row);
		outputvalue.set(myoutput);
		context.write(outputkey,outputvalue);
		
	}
}
	public static void main(String agrs[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		Job job=Job.getInstance(conf,"work");
		
		job.setJarByClass(mapside.class);
		job.setMapperClass(Myclass.class);
		
		job.addCacheFile(new Path("salary.txt").toUri());
		job.addCacheFile(new Path("desig.txt").toUri());
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(agrs[0]));
		FileSystem.get(conf).delete(new Path(agrs[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(agrs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
