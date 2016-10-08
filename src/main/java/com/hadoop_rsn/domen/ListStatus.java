package com.hadoop_rsn.domen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;

public class ListStatus {

    public static Path[] getStatus(String uri, String[] arg) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path[] paths = new Path[arg.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(arg[i]);
        }

        FileStatus[] status = fs.listStatus(paths);
        Path[] listedPaths = FileUtil.stat2Paths(status);

        return listedPaths;
    }

}
