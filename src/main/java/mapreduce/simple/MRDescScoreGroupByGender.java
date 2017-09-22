package mapreduce.simple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * List score with desc group by age
 * @author jangz
 *
 */
public class MRDescScoreGroupByGender extends Configured implements Tool {
	
	// map data, let score as key, other value sequence as value
	public static class MRDescScoreGroupByAgeMapper extends Mapper<Object, Text, IntWritable, Text> {
		private IntWritable outKey = new IntWritable();
		private Text outValue = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// name age gender score
			String[] infos = value.toString().split("\t");
			outKey.set(Integer.parseInt(infos[3]));
			outValue.set(infos[0] + "\t" + infos[1] + "\t" + infos[2]); // name age gender
			context.write(outKey, outValue);
		}
	}
	
	// group by age
	public static class MRDescScoreGroupByAgePartitioner extends Partitioner<IntWritable, Text> {

		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			if (numPartitions == 0) {
				return 0;
			}
			
			String[] infos = value.toString().split("\t");
			if (infos[2].equalsIgnoreCase("male")) {
				return 0;
			} else if (infos[2].equalsIgnoreCase("female")) {
				return 1;
			} else {
				return 2;
			}
		}
		
	}

	// change key/value to value/key so as to order
	public static class MRDescScoreGroupByAgeReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, key);
			}
		}
	}
	
	// sort algorithm
	public static class Comp extends IntWritable.Comparator {
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MRDescScoreGroupByGender(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Usage: MRDescScoreGroupByAge <in> [<in>...] <out>");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MRDescScoreGroupByAge");
		
		// set file input/output format
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		// set map-reduce logic
		job.setJarByClass(MRDescScoreGroupByGender.class);
		job.setMapperClass(MRDescScoreGroupByAgeMapper.class);
		job.setPartitionerClass(MRDescScoreGroupByAgePartitioner.class);
		job.setReducerClass(MRDescScoreGroupByAgeReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		// set output key/value format
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// set sort algorithm
		job.setSortComparatorClass(Comp.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
