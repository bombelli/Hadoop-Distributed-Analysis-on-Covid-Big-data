package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TotalCovidDeathsPerContinentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	private final static DoubleWritable Total_deaths_per_countryDoubleWritable = new DoubleWritable(0);
	private Text ContinentName = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		//break each case line using the "," separator
		String[] case_sample = value.toString().split(",");
		
		//Take the first object as the continent name
		String name = case_sample[0];
		
		ContinentName.set(name);
		
		//Third column is passed here since it contain the total_deaths per country
		double country_death  = Double.parseDouble(case_sample[4].trim());
		
		Total_deaths_per_countryDoubleWritable.set(country_death);
		
		context.write(ContinentName, Total_deaths_per_countryDoubleWritable);
	}

}
