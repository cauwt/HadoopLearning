package hdfs.simple;

import hdfs.util.ConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.UUID;

/**
 * Created by yachao on 17/9/25.
 */
public class HDFSExample {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        String currentPath = System.getProperty("user.dir");
        String fullPath = currentPath + File.separator + "src/main/java/hdfs/simple/" + UUID.randomUUID().toString();
//        FileSystem fs = FileSystem.get(ConfUtil.conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), new Configuration(), "jangz");

        InputStream in = fs.open(new Path("/user/jangz/input/test1.txt"));
        FileOutputStream out = new FileOutputStream(new File(fullPath));

        IOUtils.copyBytes(in, out, 2048, true);
    }
}
