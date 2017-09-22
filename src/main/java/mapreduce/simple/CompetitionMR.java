package mapreduce.simple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Group by age and get the info of male & female owning max score
 * @author jangz
 *
 */
public class CompetitionMR extends Configured implements Tool {

	public static class CompetitionMRMapper extends Mapper<Object, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// name age gender score
			String[] infos = value.toString().split("\t");
			outKey.set(infos[2]);
			outValue.set(infos[0] + "\t" + infos[1] + "\t" + infos[3]);
			context.write(outKey, outValue);
		}
	}
	
	public static class CompetitionMRPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			if (numPartitions == 0) {
				return 0;
			}
			
			String[] infos = value.toString().split("\t");
			int age = Integer.parseInt(infos[1]);
			
			if (age <= 20) {
				return 0;
			} else if (age <= 50) {
				return 1 % numPartitions;
			} else {
				return 2 % numPartitions;
			}
		}
		
	}

	public static class CompetitionMRReducer extends Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		private String name;
		private int age;
		private int maxScore = Integer.MIN_VALUE;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String gender = key.toString();
			for (Text value : values) {
				String[] infos = value.toString().split("\t");
				int score = Integer.parseInt(infos[2]);
				if (score > maxScore) {
					name = infos[0];
					age = Integer.parseInt(infos[1]);
					maxScore = score;
				}
			}
			outKey.set(name);
			outValue.set("age-" + age + ", gender-" + gender + ", maxScore-" + maxScore);
			context.write(outKey, outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CompetitionMR(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Usage: CompetitionMR <in> [<in>...] <out>");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CompetitionMR");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		job.setJarByClass(CompetitionMR.class);
		job.setMapperClass(CompetitionMRMapper.class);
		job.setPartitionerClass(CompetitionMRPartitioner.class);
		job.setReducerClass(CompetitionMRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
