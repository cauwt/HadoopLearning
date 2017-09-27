package mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRSum extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		ToolRunner.run(conf, new MRSum(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
	Job job = Job.getInstance(getConf(), "sum step");
		
		job.setJarByClass(MRSum.class);
		
		job.setMapperClass(SumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Information.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Information.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class SumMapper extends Mapper<LongWritable, Text, Text, Information> {

		private Information information = new Information();
		private Text k = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			String account = fields[0];
			double income = Double.parseDouble(fields[1]);
			double expenses = Double.parseDouble(fields[2]);
			
			k.set(account);
			information.set(account, income, expenses);
			
			context.write(k, information);
		}
	}

	public static class SumReducer extends Reducer<Text, Information, Text, Information> {
		
		private Information information = new Information();
		
		@Override
		protected void reduce(Text key, Iterable<Information> v2s, Context context)
				throws IOException, InterruptedException {
			double in_sum = 0;
			double out_sum = 0;
			
			for (Information info : v2s) {
				in_sum += info.getIncome();
				out_sum += info.getExpenses();
			}
			information.set("", in_sum, out_sum);
			
			context.write(key, information);
		}
	}
}
