package mapreduce.topk;

import java.io.IOException;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * group by sex and descend by score
 * @author jangz
 *
 */
public class TopK extends Configured implements Tool {
	
	public static class TopKMapper extends Mapper<Object, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] infos = value.toString().split("\t"); // name age sex score
			outKey.set(infos[2]);
			outValue.set(infos[0] + "\t" + infos[1] + "\t" + infos[3]); // name age score
			context.write(outKey, outValue);
		}
	}
	
	public static class TopKReducer extends Reducer<Text, Text, NullWritable, Text> {
		private static final int k = 3;
		private TreeMap<MyScore, Text> maleTree = new TreeMap<>((o1, o2) -> o2.compareTo(o1));
		private TreeMap<MyScore, Text> femaleTree = new TreeMap<>((o1, o2) -> o2.compareTo(o1));
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String[] infos = value.toString().split("\t");
				StringBuffer buffer = new StringBuffer("");
				// name age gender score
				buffer.append(infos[0]).append("\t").append(infos[1]).append("\t").append(key.toString()).append("\t").append(infos[2]);
				if (key.toString().equalsIgnoreCase("male")) {
					maleTree.put(new MyScore(Integer.parseInt(infos[2])), new Text(buffer.toString()));
					if (maleTree.size() > k) {
						maleTree.remove(maleTree.lastKey());
					}
				} else if (key.toString().equalsIgnoreCase("female")) {
					femaleTree.put(new MyScore(Integer.parseInt(infos[2])), new Text(buffer.toString()));
					if (femaleTree.size() > k) {
						femaleTree.remove(femaleTree.lastKey());
					}
				}
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Stream.of(maleTree.entrySet().stream(), femaleTree.entrySet().stream()).flatMap(Function.identity()).forEach(tree -> {
				try {
					context.write(NullWritable.get(), tree.getValue());
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			});
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TopK(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Usage: TopK <in> [<in>...] <out>");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopK");
		
		// set file input/output format
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		// set map-reduce logic
		job.setJarByClass(TopK.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// set output key/value format
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
