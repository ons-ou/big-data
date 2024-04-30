package hadoop.reducers;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bson.BasicBSONObject;

import java.io.IOException;

public class GenreCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private MultipleOutputs<Text, IntWritable> multipleOutputs;

    @Override
    public void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    private IntWritable result = new IntWritable();
    private BSONWritable bsonResult = new BSONWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        System.out.println(sum);
        result.set(sum);
        BasicBSONObject bsonObject = new BasicBSONObject();
        bsonObject.put("genre", key.toString().split(",")[0].trim());
        bsonObject.put("year", key.toString().split(",")[1].trim());
        bsonObject.put("count", sum);

        bsonResult.setDoc(bsonObject);
        multipleOutputs.write("combiner", NullWritable.get(), bsonResult, "by_year/part");

        String genre = key.toString().split(",")[0];
        key.set(genre.trim());

        context.write(key, result);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
