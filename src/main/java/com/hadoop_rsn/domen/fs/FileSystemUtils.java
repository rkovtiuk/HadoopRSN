package com.hadoop_rsn.domen.fs;

import com.hadoop_rsn.core.fs.HadoopFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class FileSystemUtils implements HadoopFileSystem{



    public static void getStream(String uri, OutputStream os) throws IOException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), configuration);

        try(FSDataInputStream in = fs.open(new Path(uri))) {
            IOUtils.copyBytes(in, os, 4096, false);
            in.seek(0); // back to begin of file
            IOUtils.copyBytes(in, os, 4096, false);
        }
    }

    public static void copyFileToInHadoopFS(String loacalSrc, String dst, OutputStream os) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(loacalSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);

        OutputStream out = fs.create(new Path(dst), () -> {
            try {
                os.write("...".getBytes("UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        IOUtils.copyBytes(in, os, 4096, true);
    }



}
