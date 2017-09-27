package mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * <p>Title: MRSort</p>
 * <p>Description: </p>
 * @author jangz
 * @date 2017年9月27日 下午3:03:54
 */
public class MRSort {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MRSort.class);
		
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(Information.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Information.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
	
	public static class SortMapper extends Mapper<LongWritable, Text, Information, NullWritable> {

		private Information information = new Information();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");

			String account = fields[0];
			double income = Double.parseDouble(fields[1]);
			double expenses = Double.parseDouble(fields[2]);

			information.set(account, income, expenses);
			context.write(information, NullWritable.get());
		}
	}

	public static class SortReducer extends Reducer<Information, NullWritable, Text, Information> {
		
		private Text key = new Text();
		
		@Override
		protected void reduce(Information information, Iterable<NullWritable> v2s, Context context)
				throws IOException, InterruptedException {
			String account = information.getAccount();
			key.set(account);
			context.write(key, information);
		}
	}
}
