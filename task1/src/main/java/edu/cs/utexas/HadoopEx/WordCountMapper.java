package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] words = value.toString().split(",");
		if (words.length != 17) {	
			context.write(new Text("Line Errors"), counter);
			return;
		}
		Text time = new Text();
		try{
			time = new Text(words[2].split(" ")[1].split(":")[0]);
		} catch (NumberFormatException e) {
			context.write(new Text("Line Errors"), counter);
			return;
		}
		try{
			double pLO = Double.parseDouble(words[6]);
			double pLA = Double.parseDouble(words[7]);
			double dLO = Double.parseDouble(words[8]);
			double dLA = Double.parseDouble(words[9]);
			if (pLA < 39 || pLA > 42 || pLO > -73 || pLO < -75 || dLA < 39 || dLA > 42 || dLO > -73 || dLO < -75) {
				context.write(time, new IntWritable(1));
			}
			else {
				context.write(time, new IntWritable(0));
			}
		}
		catch (NumberFormatException e) {
			context.write(new Text("Line Errors"), counter);
			return;
		}
	}
}