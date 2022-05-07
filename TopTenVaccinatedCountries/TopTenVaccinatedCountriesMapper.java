package org.myorg;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenVaccinatedCountriesMapper  extends Mapper<Object,Text, Text, LongWritable>
{

		private TreeMap<Long, String> tmap;

		@Override
		public void setup(Context context) throws IOException,InterruptedException
		{
			tmap = new TreeMap<Long, String>();
		}

		@Override
		public void map(Object key, Text value,Context context) throws IOException,
						InterruptedException
		{

			
			
			// splitting the input data
			String[] case_sample = value.toString().split(",");

			String location_name = case_sample[1];;
			//long num_of_fully_vacc_people = Long.parseLong(case_sample[6].trim());
			
			long num_of_fully_vacc_people = (long)Double.parseDouble(case_sample[6].trim());
			
			//double country_vaccinated_people  = Double.parseDouble(case_sample[6].trim());

			// insert data into treeMap,
			tmap.put(num_of_fully_vacc_people, location_name);

			
			if (tmap.size() > 10)
			{
				tmap.remove(tmap.firstKey());
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
										InterruptedException
		{
			for (Map.Entry<Long, String> entry : tmap.entrySet())
			{

				long count = entry.getKey();
				String name = entry.getValue();

				context.write(new Text(name), new LongWritable(count));
			}
		}
	


}
