package hadoop.mapreduce;

import com.mongodb.hadoop.BSONFileOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import hadoop.mappers.AvgReviewMapper;
import hadoop.reducers.AvgBsonReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
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
        job.setReducerClass(AvgBsonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setOutputFormatClass(BSONFileOutputFormat.class);

        return job;
    }
}
