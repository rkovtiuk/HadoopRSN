package util;

// cc MaxTempByStationNameUsingDistributedCacheFile Application to find the maximum temperature by station, showing station names from a lookup table passed as a distributed cache file
import java.io.File;
import java.io.IOException;

import core.common.JobBuilder;
import core.common.NcdcRecordParser;
import core.common.NcdcStationMetadata;
import core.reduce.MaxTempReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

// vv MaxTempByStationNameUsingDistributedCacheFile
public class MaxTempByStationNameUsingDistributedCacheFile
        extends Configured implements Tool {

    static class StationTempMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new Text(parser.getStationId()),
                        new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    static class MaxTempReducerWithStationLookup
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /*[*/private NcdcStationMetadata metadata;/*]*/

        /*[*/@Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            metadata = new NcdcStationMetadata();
            metadata.initialize(new File("stations-fixed-width.txt"));
        }/*]*/

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {

      /*[*/String stationName = metadata.getStationName(key.toString());/*]*/

            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(new Text(/*[*/stationName/*]*/), new IntWritable(maxValue));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(StationTempMapper.class);
        job.setCombinerClass(MaxTempReducer.class);
        job.setReducerClass(MaxTempReducerWithStationLookup.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}