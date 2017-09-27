package hdfs.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

/**
 * Created by yachao on 17/9/25.
 */
public class RPCServer implements BizProtocol {

    public static void main(String[] args) throws IOException {
        Server server = new RPC.Builder(new Configuration())
                .setInstance(new RPCServer())
                .setProtocol(BizProtocol.class)
                .setBindAddress("10.211.55.100")
                .setPort(9000)
                .build();
        server.start();
    }

    @Override
    public String sayHi(String name) {
        return "Hi~" + name;
    }
}
