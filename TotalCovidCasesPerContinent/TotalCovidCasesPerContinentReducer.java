package org.myorg;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalCovidCasesPerContinentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException
	{
		double continent_total_cases=0; //Total covid cases for a continent
		
		for (DoubleWritable value : values)
		{
			continent_total_cases += value.get();
		}
		
		context.write(key, new DoubleWritable(continent_total_cases));
	}

}
