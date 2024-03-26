package hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface MapReduceJob {

    Job createJob() throws IOException;
}
