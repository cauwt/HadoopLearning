package mapreduce.topk2.top;

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
import org.apache.log4j.Logger;

public class Top3GroupByGenderExample extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(Top3GroupByGenderExample.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			log.error("Usage: Top3GroupByGenderExample <in> <out>");
			System.exit(2);
		}

		ToolRunner.run(conf, new Top3GroupByGenderExample(), otherArgs);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		Path outPath = new Path(args[1]);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		Job job = Job.getInstance(getConf(), "Top3GroupByGenderExampleJob");

		job.setJarByClass(Top3GroupByGenderExample.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Document.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(2);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Document.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, outPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Document, NullWritable> {

		private Document document = new Document();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			log.info("MyMapper in<" + key.get() + "," + value.toString() + ">");

			String line = value.toString();
			String[] infos = line.split("\t");

			String name = infos[0];
			Integer age = Integer.parseInt(infos[1]);
			String gender = infos[2];
			Integer score = Integer.parseInt(infos[3]);

			document.set(name, age, gender, score);
			context.write(document, NullWritable.get());
			log.info("MyMapper out<" + document + ">");
		}
	}

	public static class MyPartitioner extends Partitioner<Document, NullWritable> {

		@Override
		public int getPartition(Document key, NullWritable value, int numPartitions) {
			String gender = key.getGender();
			return (gender.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class MyReducer extends Reducer<Document, NullWritable, Document, NullWritable> {

		private int k = 3;
		private int counter = 0;

		@Override
		protected void reduce(Document key, Iterable<NullWritable> v2s, Context context)
				throws IOException, InterruptedException {

			log.info("MyReducer in<" + key + ">");

			if (counter < k) {
				context.write(key, NullWritable.get());
				counter += 1;

				log.info("MyReducer out<" + key + ">");
			}
		}
	}
}
