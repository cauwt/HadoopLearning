package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by yachao on 17/9/9.
 */
public class HDFSOper {
    private static final Logger log = Logger.getLogger("HDFSOper");

    private boolean copyFile2HDFS(String srcFile, String destFile) {
        boolean result = true;
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path srcPath = new Path(srcFile);
            Path destPath = new Path(destFile);
            hdfs.copyFromLocalFile(srcPath, destPath);
        } catch (IOException e) {
            result = false;
            log.info("Copy from local file to HDFS generates an error!");
        }
        return true;
    }

    private boolean createHDFSFile(String fileName) {
        boolean result = true;
        byte[] buffer = new byte[1024];
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path path = new Path(fileName);
            FSDataOutputStream fsDataOutputStream = hdfs.create(path);
            fsDataOutputStream.write(buffer, 0, buffer.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
