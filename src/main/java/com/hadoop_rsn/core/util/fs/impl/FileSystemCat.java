package com.hadoop_rsn.core.util.fs.impl;

import com.hadoop_rsn.core.util.fs.HadoopFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class FileSystemCat implements HadoopFileSystem{

    public void getStream(String uri) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        try(InputStream in = fs.open(new Path(uri))) {
            IOUtils.copyBytes(in, System.out, 4096, false);
        }
    }

}
