package hadoop.mapreduce;

import hadoop.mappers.AvgReviewByGenreMapper;
import hadoop.reducers.AvgReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class AvgReviewByGenre implements MapReduceJob {
    @Override
    public Job createJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Average Review By Genre");
        job.setJarByClass(MapReduceMain.class);
        job.setMapperClass(AvgReviewByGenreMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);


        return job;
    }
}
