package hadoop.mappers;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.NamesExtractor;

import java.io.IOException;
import java.util.List;

public class AvgReviewMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private FloatWritable review = new FloatWritable();
    private Text company = new Text();

    public void map(Object key, Text value, Context context
    ) {
        CSVParser csvParser = new CSVParser();
        try {
            String[] tokens = csvParser.parseLine(value.toString());
            assert tokens.length == 23;
            String companies = tokens[12];
            float revValue = Float.parseFloat(tokens[22]);

            List<String> companyNames = NamesExtractor.extractName(companies);

            review.set(revValue);

            for (String val : companyNames) {
                company.set(val);
                System.out.println(val + " " + review.get());
                context.write(company, review);
            }
        } catch (Exception ex){}

    }
}
