package mapreduce.partner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by yachao on 17/9/27.
 */
public class ContestSort extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ContextPerson2 <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        ToolRunner.run(conf, new ContestSort(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "contest sort");

        job.setJarByClass(ContestSort.class);

        job.setMapperClass(ContestMapper.class);
        job.setMapOutputKeyClass(ContestPerson.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(ContestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ContestPerson.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ContestMapper extends Mapper<LongWritable, Text, ContestPerson, NullWritable> {

        private ContestPerson person = new ContestPerson();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] infos = line.split(",");

            String name = infos[0];
            Integer age = Integer.parseInt(infos[1]);
            String gender = infos[2];
            Integer score = Integer.parseInt(infos[3]);

            person.set(name, age, gender, score);

            context.write(person, NullWritable.get());
        }
    }

    public static class ContestReducer extends Reducer<ContestPerson, NullWritable, Text, ContestPerson> {

        private Text k = new Text();

        @Override
        protected void reduce(ContestPerson key, Iterable<NullWritable> values, Context context) throws IOException,
                InterruptedException {
            String name = key.getName();
            k.set(name);
            context.write(k, key);
        }
    }

}
