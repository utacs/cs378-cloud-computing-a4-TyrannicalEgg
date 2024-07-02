package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	// Create a counter and initialize with 1
	private final DoubleWritable counter = new DoubleWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] words = value.toString().split(",");
		if (words.length != 17) {	
			context.write(new Text("Line Errors"), new DoubleWritable(0.1));
			return;
		}
		Text taxi = new Text(words[0]);
		try{
			double pLO = Double.parseDouble(words[6]);
			double pLA = Double.parseDouble(words[7]);
			double dLO = Double.parseDouble(words[8]);
			double dLA = Double.parseDouble(words[9]);
			if (pLA < 39 || pLA > 42 || pLO > -73 || pLO < -75 || dLA < 39 || dLA > 42 || dLO > -73 || dLO < -75) {
				context.write(taxi, new DoubleWritable(1));
			}
			else {
				context.write(taxi, new DoubleWritable(0));
			}
		}
		catch (NumberFormatException e) {
			context.write(new Text("Line Errors"), new DoubleWritable(0.1));
			return;
		}
	}
}
