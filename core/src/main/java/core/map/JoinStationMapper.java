package core.map;

import java.io.IOException;

import core.common.NcdcStationMetadataParser;
import core.model.TextPair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

//Created by RSN on 15.10.2016.
public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (parser.parse(value)) {
            context.write(new TextPair(parser.getStationId(), "0"),
                    new Text(parser.getStationName()));
        }
    }
}
