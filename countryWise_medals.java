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

public class countryWise_medals {
	public static class mapclass extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String[] str=value.toString().split("\t");
			String country=str[2];
			int mdls=Integer.parseInt(str[9]);
			if(str[5].equals("Swimming"))
			{
			con.write(new Text(country), new IntWritable(mdls));
			}
		}
	}
	public static class reduceclass extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> value,Context con) throws IOException, InterruptedException
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
		job.setJarByClass(countryWise_medals.class);
		job.setMapperClass(mapclass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(reduceclass.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
