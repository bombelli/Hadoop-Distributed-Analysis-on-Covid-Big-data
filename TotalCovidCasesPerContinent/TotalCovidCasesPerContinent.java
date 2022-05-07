package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class TotalCovidCasesPerContinent 
{
	public static void main(String[] args) throws Exception
	{
	Configuration conf = new Configuration();
	if (args.length != 3)
	{
		System.err.println("Usage: TotalCovidCasesPerContinent <input path> <output path>");
		System.exit(-1);
	}
	
	Job job;
	job=Job.getInstance(conf, "Total Covid Cases Per Continent");
	job.setJarByClass(TotalCovidCasesPerContinent.class);
	
	FileInputFormat.addInputPath(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	
	job.setMapperClass(TotalCovidCasesPerContinentMapper.class);
	job.setReducerClass(TotalCovidCasesPerContinentReducer.class);
	job.setCombinerClass(TotalCovidCasesPerContinentReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	
	// Delete output if exists
	FileSystem hdfs = FileSystem.get(conf);
	Path outputDir = new Path(args[2]);
	
	if (hdfs.exists(outputDir))
		hdfs.delete(outputDir, true);
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
