package hadoop.reducers;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import java.io.IOException;

public class SumBsonReducer extends Reducer<Text, IntWritable, NullWritable, BSONWritable> {

    private BSONWritable result = new BSONWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        BasicBSONObject bsonObject = new BasicBSONObject();
        bsonObject.put("name", key.toString());
        bsonObject.put("count", sum);

        System.out.println("result->" + bsonObject);
        result.setDoc(bsonObject);

        context.write(NullWritable.get(), result);
    }
}
