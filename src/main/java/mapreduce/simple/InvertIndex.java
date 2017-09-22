package mapreduce.simple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertIndex {

	public static class InvertIndexMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			StringTokenizer tokenizer = new StringTokenizer(key.toString());
			while (tokenizer.hasMoreTokens()) {
				// For each word emit word as key and file name as value 
				context.write(new Text(tokenizer.nextToken()), new Text(fileName));
			}
		}
	}

	public static class InvertIndexReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Declare the Hash Map to store file name as key to compute
			// and store number of times the filename occurred for as value
			Map<String, Integer> map = new HashMap<>();
			for (Text value : values) {
				String file = value.toString();
				if (map.containsKey(file)) {
					map.put(file, map.get(file) + 1);
				} else {
					map.put(file, 1);
				}
			}
			context.write(key, new Text(map.toString()));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.out.println("Usage: InvertIndex <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "invert index");
		job.setJarByClass(InvertIndex.class);
		job.setMapperClass(InvertIndexMapper.class);
		job.setCombinerClass(InvertIndexReducer.class);
		job.setReducerClass(InvertIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
