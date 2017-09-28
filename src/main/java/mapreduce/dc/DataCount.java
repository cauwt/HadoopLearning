package mapreduce.dc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
        protected void reduce(Text key, Iterable<DataBean> v2s, Context context) throws IOException,
                InterruptedException {
        	// Define counter
            long up_sum = 0;
            long down_sum = 0;
            
            // Iterator v2s, then sum
            for (DataBean value : v2s) {
                up_sum += value.getUpPayLoad();
                down_sum += value.getDownPayLoad();
            }
            DataBean bean = new DataBean("", up_sum, down_sum);
            context.write(key, bean);
        }
    }
    
    public static class ServiceProviderPartitioner extends Partitioner<Text, DataBean> {
    	
    	private static Map<String, Integer> providerMap = new HashMap<String, Integer>();
    	
    	static {
    		providerMap.put("139", 1);
    		providerMap.put("138", 2);
    		providerMap.put("159", 3);
    	}
    	
		@Override
		public int getPartition(Text key, DataBean value, int numPartitions) {
			String telNo = key.toString();
			String pcode = telNo.substring(0, 3);
			Integer p = providerMap.get(pcode);
			
			if (Objects.isNull(p)) {
				p = 0;
			}
			return p;
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

//        job.setPartitionerClass(ServiceProviderPartitioner.class);
        
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        job.waitForCompletion(true);
    }
}
