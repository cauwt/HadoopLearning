package mapreduce.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> v2s, Context context)
			throws IOException, InterruptedException {
		// Define a counter
		long sum = 0;
		
		for (LongWritable value : v2s) {
			sum += value.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
