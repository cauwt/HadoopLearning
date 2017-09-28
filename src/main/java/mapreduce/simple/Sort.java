package mapreduce.simple;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {

	public static class Map extends Mapper<Object, Text, IntWritable, Text> {

		IntWritable outKey = new IntWritable();
		Text outValue = new Text();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				String element = st.nextToken();
				if (Pattern.matches("\\d+", element)) {
					outKey.set(Integer.parseInt(element));
				} else {
					outValue.set(element);
				}
			}

			context.write(outKey, outValue);
		}

	}

	public static class Combine extends Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			for (Text text : arg1) {
				arg2.write(arg0, text);
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, Text, IntWritable> {

		String[] topK = null;
		int count = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = new String[Integer.parseInt(conf.get("k"))];
		}

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(text, key);
				if (count < 10) {
					topK[count++] = text.toString();
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String topKout = conf.get("topKout");
			Path topKoutPath = new Path(topKout);
			FileSystem fs = topKoutPath.getFileSystem(conf);
			FSDataOutputStream fsDOS = fs.create(topKoutPath, true);
			for (int i = 0; i < topK.length; i++) {
				fsDOS.write(topK[i].getBytes(), 0, topK[i].length());
				fsDOS.write("\r".getBytes());
			}
			fsDOS.flush();
			fsDOS.close();
		}

	}

	public static class Comp1 extends IntWritable.Comparator {

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}

	}

	@SuppressWarnings("deprecation")
	public static void run(String in, String out, String topKout, int k)
			throws IOException, ClassNotFoundException, InterruptedException {

		Path outPath = new Path(out);

		Configuration conf = new Configuration();

		conf.set("topKout", topKout);
		conf.set("k", k + "");

		Job job = new Job(conf, "Sort");

		job.setJarByClass(Sort.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setSortComparatorClass(Comp1.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, outPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
