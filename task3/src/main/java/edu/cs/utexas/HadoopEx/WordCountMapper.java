package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import TimePriceWrapper;

public class WordCountMapper extends Mapper<Object, Text, Text, TimePriceWrapper> {

	// Create a counter and initialize with 1
	private final DoubleWritable counter = new DoubleWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] words = value.toString().split(",");
		if (words.length != 17) {
			context.write(new Text("Line Error") , new TimePriceWrapper(new IntWritable(1), new DoubleWritable(1)));
			return;
		}
		Text driver = new Text(words[1]);
		try{
			double price = Double.parseDouble(words[16]);
			int time = Integer.parseInt(words[4]);
			if (price <= 0 || time <= 0) {
				context.write(new Text("Line Error"), new TimePriceWrapper(new IntWritable(1), new DoubleWritable(1)));
				return;
			}
			TimePriceWrapper tpw = new TimePriceWrapper(new IntWritable(time), new DoubleWritable(price));
			context.write(driver, tpw);
		}
		catch (NumberFormatException e) {
			context.write(new Text("Line Error") , new TimePriceWrapper(new IntWritable(1), new DoubleWritable(1)));
			return;
		}
	}
}