package core;

import core.converter.SmallFilesToSequenceFileConverter;
import core.fs.out.PartitionByStationUsingMultipleOutputs;
import core.fs.out.PartitionByStationYearUsingMultipleOutputs;
import core.reduce.MinimalMapReduceWithDefaults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

// FIXME: 13.10.2016 
public class JobBuilder {

    public static Job parseInputAndOutput(
            PartitionByStationYearUsingMultipleOutputs partitionByStationYearUsingMultipleOutputs,
            Configuration conf,
            String[] args) {
        return null;
    }

    public static Job parseInputAndOutput(
            MinimalMapReduceWithDefaults minimalMapReduceWithDefaults,
            Configuration conf,
            String[] strings) {
        return null;
    }

    public static Job parseInputAndOutput(
            PartitionByStationUsingMultipleOutputs partitionByStationUsingMultipleOutputs,
            Configuration conf,
            String[] args) {
        return null;
    }

    public static Job parseInputAndOutput(SmallFilesToSequenceFileConverter smallFilesToSequenceFileConverter, Configuration conf, String[] args) {
        return null;
    }
}
