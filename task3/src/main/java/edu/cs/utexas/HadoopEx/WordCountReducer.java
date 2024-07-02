package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import TimePriceWrapper;

public class WordCountReducer extends  Reducer<Text, TimePriceWrapper, Text, DoubleWritable> {

   public void reduce(Text text, Iterable<TimePriceWrapper> values, Context context)
           throws IOException, InterruptedException {
	   
       int sumTime = 0;
       double sumCost = 0.0;
       
       for (TimePriceWrapper value : values) {
           sumTime += value.getTime().get();
           sumCost += value.getPrice().get();
       }
       
       context.write(text, new DoubleWritable(sumCost/sumTime*60.0));
   }
}
