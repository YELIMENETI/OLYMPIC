import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

//no of Orders & TotalSales for Customer-RetailData

public class IndiaWise {
	public static class mapclass extends Mapper<LongWritable,Text,IntWritable,IntWritable>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String[] str=value.toString().split("\t");
			String sport=str[5];
			int mdls=Integer.parseInt(str[9]);
			int year =Integer.parseInt(str[3]);
			if(str[2].equals("India"))
			{
			con.write(new IntWritable(year), new IntWritable(mdls));
			}
		}
	}
	public static class reduceclass extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
	{
		public void reduce(IntWritable key,Iterable<IntWritable> value,Context con) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable val:value)
			{
			sum+=val.get();
			}
			con.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"");
		job.setJarByClass(IndiaWise.class);
		job.setMapperClass(mapclass.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(reduceclass.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
