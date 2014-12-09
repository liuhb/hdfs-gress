package com.sponge.srd.hdfsgress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by liuhb on 2014/12/9.
 */
public class Utils {

    public synchronized  static void hdfsAppend(Path srcFile, Path distFile, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(distFile.toString()), conf);
        Path destFile = new Path(distFile.toString() + ".process");
        if ( fs.exists(distFile)) {
            fs.rename(distFile,destFile);
        } else {
            IOUtils.closeStream(fs.create(destFile));
        }
        FSDataOutputStream append = fs.append(destFile);
        InputStream in = fs.open(srcFile);
        try {
            IOUtils.copyBytes(in, append, 4096);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(append);
        }
        fs.rename(destFile, distFile);
    }
}
