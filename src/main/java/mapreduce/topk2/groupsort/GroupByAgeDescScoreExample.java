package mapreduce.topk2.groupsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * <p>Title: GroupByAgeDescScoreExample</p>
 * <p>Description: </p>
 * @author jangz
 * @date 2017/9/29 14:14
 */
public class GroupByAgeDescScoreExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.out.println("Usage: GroupByAgeDescScoreExample <in> <out>");
			System.exit(2);
		}

		ToolRunner.run(conf, new GroupByAgeDescScoreExample(), otherArgs);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		Path outPath = new Path(args[1]);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		Job job = Job.getInstance(getConf(), "GroupByAgeDescScoreExampleJob");

		job.setJarByClass(GroupByAgeDescScoreExample.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(3);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Person.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, outPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Person, NullWritable> {

		private Person person = new Person();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("MyMapper in<" + key.get() + "," + value.toString() + ">");

			String line = value.toString();
			String[] infos = line.split("\t");

			String name = infos[0];
			Integer age = Integer.parseInt(infos[1]);
			String gender = infos[2];
			Integer score = Integer.parseInt(infos[3]);

			person.set(name, age, gender, score);
			context.write(person, NullWritable.get());
			System.out.println("MyMapper out<" + person + ">");
		}
	}
	
	public static class MyPartitioner extends Partitioner<Person, NullWritable> {

		@Override
		public int getPartition(Person key, NullWritable value, int numPartitions) {
			
			Integer age = key.getAge();
			
			if (age < 20) {
				return 0;
			} else if (age <= 50) {
				return 1;
			} else {
				return 2;
			}
		}
	}

	public static class MyReducer extends Reducer<Person, NullWritable, Person, NullWritable> {

		private Text k = new Text();

		@Override
		protected void reduce(Person key, Iterable<NullWritable> v2s, Context context)
				throws IOException, InterruptedException {
			System.out.println("MyReducer in<" + key + ">");

			context.write(key, NullWritable.get());

			System.out.println("MyReducer out<" + k + "," + key + ">");
		}
	}
}
