package core.map;

import core.parser.NcdcRecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//Created by RSN on 13.10.2016.
public class MaxTempMapperV2 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        parser.parse((javax.xml.soap.Text) value);
        if (parser.isValidTemp()) {
            context.write(new Text(parser.getYear()),
                    new IntWritable(parser.getAirTemp()));
        }
    }
}
