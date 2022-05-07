package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopTenVaccinatedCountries 
{
	public static void main(String[] args) throws Exception
	{
	Configuration conf = new Configuration();
	if (args.length != 3)
	{
		System.err.println("Usage: TopTenVaccinatedCountries  <input path> <output path>");
		System.exit(-1);
	}
	
	Job job;
	job=Job.getInstance(conf, "Top Ten Vaccinated Countries ");
	job.setJarByClass(TopTenVaccinatedCountries.class);
	
	FileInputFormat.addInputPath(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	
	job.setMapperClass(TopTenVaccinatedCountriesMapper.class);
	job.setReducerClass(TopTenVaccinatedCountriesReducer.class);
	
	
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
	
	// Delete output if exists
	FileSystem hdfs = FileSystem.get(conf);
	Path outputDir = new Path(args[2]);
	
	if (hdfs.exists(outputDir))
		hdfs.delete(outputDir, true);
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
