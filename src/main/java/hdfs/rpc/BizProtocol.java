package hdfs.rpc;

/**
 * Created by yachao on 17/9/25.
 */
public interface BizProtocol {

    public static final long versionID = 10010L;

    public String sayHi(String name);
}
