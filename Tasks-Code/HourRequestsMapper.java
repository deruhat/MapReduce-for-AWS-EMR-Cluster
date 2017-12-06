// Task 3
package aws.emr.wordcount;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HourRequestMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Text hour = new Text();

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			// RequestCount: count how many request in a specific hour
			String wordString = word.toString();
			if(wordString.matches(("\\d{2}:\\d{2}:\\d{2}"))){
				String hourString = "";
				StringBuilder sb = new StringBuilder();
				sb.append(wordString.charAt(0));
				sb.append(wordString.charAt(1));
				hourString = sb.toString();
				hour.set(hourString);
				output.collect(hour, one);
			}
		}
	}
}