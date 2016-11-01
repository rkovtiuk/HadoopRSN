package core.map;

import java.io.IOException;

import core.common.NcdcRecordParserMax;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapperV4 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParserMax parser = new NcdcRecordParserMax();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        parser.parse(value);
        if (parser.isValidTemp()) {
            int airTemperature = parser.getAirTemp();
            context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
        } else if (parser.isMalformedTemp()) {
            System.err.println("Ignoring possibly corrupt input: " + value);
            context.getCounter(Temp.MALFORMED).increment(1);
        }
    }

    enum Temp {
        MALFORMED
    }
}
