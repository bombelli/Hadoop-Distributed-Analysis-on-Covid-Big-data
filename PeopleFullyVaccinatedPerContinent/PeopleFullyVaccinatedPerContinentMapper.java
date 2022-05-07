package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PeopleFullyVaccinatedPerContinentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	private final static DoubleWritable people_vaccinated_per_countryDoubleWritable = new DoubleWritable(0);
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
		
		//Third column is passed here since it contain the people vaccinated per country
		double country_vaccinated_people  = Double.parseDouble(case_sample[6].trim());
		
		people_vaccinated_per_countryDoubleWritable.set(country_vaccinated_people);
		
		context.write(ContinentName, people_vaccinated_per_countryDoubleWritable);
	}

}
