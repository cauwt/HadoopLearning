package mapreduce.topk2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Top3GroupByGenderExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.out.println("Usage: Top3GroupByGenderExample <in> <out>");
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

		Job job = Job.getInstance(getConf(), "Top3GroupByGenderExampleJob");

		job.setJarByClass(Top3GroupByGenderExample.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Person.class);
		FileOutputFormat.setOutputPath(job, outPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Person> {

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

			context.write(new Text(gender), person);
			System.out.println("MyMapper out<" + gender + "," + person + ">");
		}
	}

	public static class MyReducer extends Reducer<Text, Person, Text, Person> {

		private List<Person> maleList = new ArrayList<>();
		private List<Person> femaleList = new ArrayList<>();

		@Override
		protected void reduce(Text key, Iterable<Person> v2s, Context context)
				throws IOException, InterruptedException {

			for (Person person : v2s) {
				System.out.println("MyReducer in<" + key + "," + person + ">");

				if (key.equals("male")) {
					maleList.add(person);
				} else {
					femaleList.add(person);
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Comparator<Person> comparator = (Person o1, Person o2) -> {
					if (o1.getScore() > o2.getScore()) {
						return -1;
					}
					return 1;
			};
			Collections.sort(maleList, comparator);
			Collections.sort(femaleList, comparator);
			
			int k = 3;
			
			k = maleList.size() >= 3 ? k : maleList.size();
			for (int i = 0; i < k; i++) {
				Person person = maleList.get(i);
				context.write(new Text(person.getName()), person);
				
				System.out.println("MyReducer out<" + person.getName() + "," + person + ">");
			}
			
			k = femaleList.size() >= 3 ? k : femaleList.size();
			for (int i = 0; i < k; i++) {
				Person person = maleList.get(i);
				context.write(new Text(person.getName()), person);
				
				System.out.println("MyReducer out<" + person.getName() + "," + person + ">");
			}
		}
	}
}
