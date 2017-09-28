package mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import mapreduce.dc.DataBean;

public class KpiPartitioner extends Partitioner<Text, DataBean> {

	@Override
	public int getPartition(Text key, DataBean value, int numPartitions) {
		int numLength = key.toString().length();
		
		if (numLength == 11) {
			return 0;
		}
		return 1;
	}

}
