package hadoop.mapreduce;

import hadoop.mappers.GenreMapper;
import hadoop.reducers.GenreCombiner;
import hadoop.reducers.SumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class DistributionByGenre implements MapReduceJob {

    @Override
    public Job createJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Distribution of Genres");
        job.setJarByClass(MapReduceMain.class);
        job.setMapperClass(GenreMapper.class);
        job.setCombinerClass(GenreCombiner.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }
}
