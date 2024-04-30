package hadoop.mapreduce;

import com.mongodb.hadoop.BSONFileOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import hadoop.mappers.GenreMapper;
import hadoop.reducers.GenreCombiner;
import hadoop.reducers.SumBsonReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
        job.setReducerClass(SumBsonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(BSONFileOutputFormat.class);
        return job;
    }
}