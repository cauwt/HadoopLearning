package hdfs;

import hdfs.util.ConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Objects;

/**
 * Created by yachao on 17/9/10.
 */
public class HdfsEx {

    private static final Logger log = Logger.getLogger(HdfsEx.class);

    private static void testMkdirPath(String path) throws IOException {
        FileSystem fs = null;

        log.info("Creating " + path + " on hdfs.");
        Path myPath = new Path(path);
        try {
            fs = FileSystem.get(ConfUtil.conf);
            fs.mkdirs(myPath);

            log.info("Creating " + path + " successfully on hdfs.");
        } catch (IOException e) {
            log.error("HdfsEx: ", e);
        } finally {
            if (Objects.nonNull(fs)) {
                fs.close();
            }
        }
    }

    private static void testDeletePath(String path) throws IOException {
        FileSystem fs = null;

        log.info("Deleting " + path + " on hdfs.");
        Path myPath = new Path(path);
        try {
            fs = myPath.getFileSystem(ConfUtil.conf);
            fs.delete(myPath, true);

            log.info("Deleting " + path + " successfully on hdfs.");
        } catch (IOException ex) {
            log.error("HdfsEx", ex);
        } finally {
            if (Objects.nonNull(fs)) {
                fs.close();
            }
        }
    }

    public static void main(String[] args) {
//        String path = "hdfs://master:9000/test/mkdirs-test";
//        String path = "/test/mkdirs-test";
        String path = "/test";
        try {
//            testMkdirPath(path);
            testDeletePath(path);
        } catch (IOException ex) {
            log.error("HdfsEx", ex);
        }
        log.info("timestamp: " + System.currentTimeMillis());
    }
}
