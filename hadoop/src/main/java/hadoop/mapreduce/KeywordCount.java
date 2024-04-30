package hadoop.mapreduce;

import com.mongodb.hadoop.BSONFileOutputFormat;
import hadoop.mappers.KeywordMapper;
import hadoop.reducers.SumBsonReducer;
import hadoop.reducers.SumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class KeywordCount implements MapReduceJob{
    @Override
    public Job createJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "KeyWordCount");
        job.setJarByClass(MapReduceMain.class);
        job.setMapperClass(KeywordMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumBsonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(BSONFileOutputFormat.class);

        return job;
    }
}
