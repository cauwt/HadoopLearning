package mapreduce.dc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yachao on 17/9/25.
 */
public class DataCount {

    public static class DataMapper extends Mapper<LongWritable, Text, Text, DataBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // accept
            String line = value.toString();
            // split
            String[] fields = line.split("\t");
            String tel = fields[1];
            long up = Long.parseLong(fields[8]);
            long down = Long.parseLong(fields[9]);
            DataBean bean = new DataBean(tel, up, down);

            // send
            context.write(new Text(tel), bean);
        }
    }

    public static class DataReduce extends Reducer<Text, DataBean, Text, DataBean> {
        @Override
        protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException,
                InterruptedException {
            long up_sum = 0;
            long down_sum = 0;

            for (DataBean value : values) {
                up_sum += value.getUpPayLoad();
                down_sum += value.getDownPayLoad();
            }
            DataBean bean = new DataBean("", up_sum, down_sum);
            context.write(key, bean);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(DataCount.class);

        job.setMapperClass(DataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setReducerClass(DataReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
