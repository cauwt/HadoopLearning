package hbase;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by yachao on 17/9/16.
 */
public class WriteToHBase {

    public static Configuration conf;

    static {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public WriteToHBase() {

    }
}
