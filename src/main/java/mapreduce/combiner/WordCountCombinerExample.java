package mapreduce.combiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountCombinerExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.out.println("Usage: WordCountCombinerExample <in> <out>");
			System.exit(2);
		}
		
		ToolRunner.run(conf, new WordCountCombinerExample(), otherArgs);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		Path outputPath = new Path(args[1]);
		
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		Job job = Job.getInstance(getConf(), "CombinerExample");
		
		job.setJarByClass(WordCountCombinerExample.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setCombinerClass(MyCombiner.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private Text k = new Text();
		private LongWritable one = new LongWritable(1L);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split("\t");

			for (String word : words) {
				k.set(word);
				context.write(k, one);

				System.out.println("Mapper output<" + word + "," + 1 + ">");
			}
		}
	}

	public static class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> v2s, Context context)
				throws IOException, InterruptedException {
			System.out.println("Combiner in<" + key.toString() + ",N(N>=1)>");
			
			long count = 0L;
			for (LongWritable value : v2s) {
				count += value.get();
				
				System.out.println("Combiner in key/value<" + key.toString() + "," + value.get() + ">");
			}
			
			context.write(key, new LongWritable(count));
			System.out.println("Combiner output key/value<" + key.toString() + "," + count + ">");
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> v2s, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer in<" + key.toString() + ",N(N>=1)>");

			long count = 0L;
			for (LongWritable value : v2s) {
				count += value.get();

				System.out.println("Reducer in key/value<" + key.toString() + "," + value.get() + ">");
			}
			context.write(key, new LongWritable(count));
		}
	}

}
