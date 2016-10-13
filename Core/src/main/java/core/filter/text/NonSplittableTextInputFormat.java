package core.filter.text;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class NonSplittableTextInputFormat extends TextInputFormat {

    private final boolean isSplittable = false;

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return isSplittable;
    }
}
