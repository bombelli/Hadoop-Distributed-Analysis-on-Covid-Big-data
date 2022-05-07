package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PeopleFullyVaccinatedPerContinent
{
	public static void main(String[] args) throws Exception
	{
	Configuration conf = new Configuration();
	if (args.length != 3)
	{
		System.err.println("Usage: PeopleFullyVaccinatedPerContinent <input path> <output path>");
		System.exit(-1);
	}
	
	Job job;
	job=Job.getInstance(conf, "People Fully Vaccinated Per Continent");
	job.setJarByClass(PeopleFullyVaccinatedPerContinent.class);
	
	FileInputFormat.addInputPath(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	
	job.setMapperClass(PeopleFullyVaccinatedPerContinentMapper.class);
	job.setReducerClass(PeopleFullyVaccinatedPerContinentReducer.class);
	job.setCombinerClass(PeopleFullyVaccinatedPerContinentReducer.class);
	
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
