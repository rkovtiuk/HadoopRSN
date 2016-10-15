package core.conf;

import core.common.NcdcRecordParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StationPartitioner extends Partitioner<LongWritable, Text> {

    private NcdcRecordParser parser= new NcdcRecordParser();

    @Override
    public int getPartition(LongWritable longWritable, Text text, int i) {
        parser.parse(text);

        return getPartition(new String(parser.getStationId()));
    }

    private int getPartition(String stationId) {
        return 0;
    }

}
