package hadoop.mapreduce;

import hadoop.mappers.AvgReviewMapper;
import hadoop.reducers.AvgReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class AvgReview implements MapReduceJob {
    @Override
    public Job createJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Distribution of Genres");
        job.setJarByClass(MapReduceMain.class);
        job.setMapperClass(AvgReviewMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        return job;
    }
}
