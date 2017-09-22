package flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.ipc.RPC;

import java.nio.charset.Charset;

/**
 * Created by yachao on 17/9/10.
 */
public class MyRpcClientFacade {

    private RpcClient rpcClient;

    private String hostname;

    private int ip;

    public void init(String hostname, int ip) {
        this.hostname = hostname;
        this.ip = ip;
        this.rpcClient = RpcClientFactory.getDefaultInstance(hostname, ip);
//        this.rpcClient = RpcClientFactory.getThriftInstance(hostname, ip);
    }

    public void sendDataToFlume(String data) {
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        try {
            rpcClient.append(event);
        } catch (EventDeliveryException e) {
            rpcClient.close();
            rpcClient = null;
            rpcClient = RpcClientFactory.getDefaultInstance(hostname, ip);
            e.printStackTrace();
        }
    }

    public void cleanUp() {
        rpcClient.close();
    }
}
