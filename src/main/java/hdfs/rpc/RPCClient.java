package hdfs.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by yachao on 17/9/25.
 */
public class RPCClient {

    public static void main(String[] args) throws IOException {
    	Configuration conf = new Configuration();
        BizProtocol proxy = RPC.getProxy(BizProtocol.class, 10010, new InetSocketAddress("10.211.55.100", 9527), conf);

        String sayHi = proxy.sayHi("Jang");
        System.out.println(sayHi);
        RPC.stopProxy(proxy);
    }
}
