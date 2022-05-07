package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PeopleFullyVaccinatedPerContinentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException
	{
		double continent_vaccinated_people=0; //Total vaccinated people for a continent
		
		for (DoubleWritable value : values)
		{
			continent_vaccinated_people += value.get();
		}
		
		context.write(key, new DoubleWritable(continent_vaccinated_people));
	}

}
