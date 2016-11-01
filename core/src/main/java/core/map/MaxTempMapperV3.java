package core.map;

import java.io.IOException;

import core.common.NcdcRecordParserMax;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapperV3 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParserMax parser = new NcdcRecordParserMax();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemp()) {
            int airTemp = parser.getAirTemp();
        if (airTemp > 1000) {
                context.setStatus("Detected possibly corrupt record: see logs.");
                context.getCounter(Temp.OVER_100).increment(1);
            }
            context.write(new Text(parser.getYear()), new IntWritable(airTemp));
        }
    }

    enum Temp{
        OVER_100
    }
}
