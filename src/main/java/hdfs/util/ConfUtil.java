package hdfs.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by yachao on 17/9/10.
 */
public class ConfUtil {

    public static final Configuration conf = new Configuration();

    static {
        conf.set("fs.defaultFS", "hdfs://master:9000");
        System.setProperty("HADOOP_USER_NAME", "jangz");
    }
}
