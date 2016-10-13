package core.core.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

import java.net.URI;

public class MapFileFixer {
    public static long mapFix(String mapUri) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(mapUri), conf);
        Path map = new Path(mapUri);
        Path mapData = new Path(map, MapFile.DATA_FILE_NAME);

        //Get key and value types from data sequence file
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);
        Class keyClass = reader.getKeyClass();
        Class valueClass = reader.getValueClass();
        reader.close();

        //Create the map file index file
        return MapFile.fix(fs, map, keyClass, valueClass, false, conf);
    }
}
