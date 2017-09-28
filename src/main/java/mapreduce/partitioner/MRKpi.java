package mapreduce.partitioner;

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

import mapreduce.dc.DataBean;

/**
 * 
 * <p>Title: MRKpi</p>
 * <p>Description: </p>
 * @author jangz
 * @date 2017年9月28日 上午10:48:10
 */
public class MRKpi extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.out.println("Usage: MRKpi <in> <out>");
			System.exit(2);
		}

		ToolRunner.run(conf, new MRKpi(), otherArgs);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		Path outPath = new Path(args[1]);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		Job job = Job.getInstance(getConf(), "MRKpiJob");

		job.setJarByClass(MRKpi.class);

		job.setMapperClass(KpiMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		/**
		 * Define a partitioner
		 */
		job.setPartitionerClass(KpiPartitioner.class);
		job.setNumReduceTasks(2);

		job.setCombinerClass(KpiReduce.class);

		job.setReducerClass(KpiReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		FileOutputFormat.setOutputPath(job, outPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class KpiMapper extends Mapper<LongWritable, Text, Text, DataBean> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] fields = line.split("\t");
			String tel = fields[1];
			long up = Long.parseLong(fields[8]);
			long down = Long.parseLong(fields[9]);
			DataBean bean = new DataBean(tel, up, down);

			context.write(new Text(tel), bean);
		}
	}

	public static class KpiReduce extends Reducer<Text, DataBean, Text, DataBean> {
		@Override
		protected void reduce(Text key, Iterable<DataBean> v2s, Context context)
				throws IOException, InterruptedException {
			long up_sum = 0;
			long down_sum = 0;

			for (DataBean value : v2s) {
				up_sum += value.getUpPayLoad();
				down_sum += value.getDownPayLoad();
			}
			DataBean bean = new DataBean("", up_sum, down_sum);
			context.write(key, bean);
		}
	}
}
