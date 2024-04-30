package hadoop.mapreduce;

import com.mongodb.hadoop.BSONFileOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class MapReduceMain {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        OperationTypes type = OperationTypes.valueOf(args[0]);

        MapReduceJob jobCreator = null;
        switch (type){
            case DIST_BY_GENRE:
                jobCreator = new DistributionByGenre();
                break;
            case AVG_RATE_BY_GENRE:
                jobCreator = new AvgReviewByGenre();
                break;
            case AVG_RATE_BY_COMPANY:
                jobCreator = new AvgReview();
                break;
            case KEYWORD_COUNT:
                jobCreator = new KeywordCount();
        }

        Job job = jobCreator.createJob();

        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path outputPath = new Path(args[2]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (type == OperationTypes.DIST_BY_GENRE) {
            MultipleOutputs.addNamedOutput(job, "combiner", BSONFileOutputFormat.class, NullWritable.class, BSONWritable.class);
            job.setOutputFormatClass(BSONFileOutputFormat.class);
        }

        job.waitForCompletion(true);
    }
}
