package core;

import core.common.JobBuilder;
import core.common.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class TempDistribution extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {

        Job job = JobBuilder.parseInputAndOutput(this, getConf(), strings);

        if (job == null) {
            return -1;
        }

        job.setMapperClass(TempCountMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class TempCountMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable>{
        private static final LongWritable ONE = new LongWritable(1);
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);

            if (parser.isValidTemp()){
                context.write(new IntWritable(parser.getAirTemp()/10), ONE);
            }
        }
    }
}
