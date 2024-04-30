package hadoop.reducers;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import java.io.IOException;

public class AvgBsonReducer
        extends Reducer<Text, FloatWritable, NullWritable, BSONWritable> {

    private BSONWritable result = new BSONWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }
        BasicBSONObject bsonObject = new BasicBSONObject();
        bsonObject.put("name", key.toString());
        bsonObject.put("count", sum/count);

        System.out.println("result->" + bsonObject);
        result.setDoc(bsonObject);

        context.write(NullWritable.get(), result);

    }
}
