package hadoop.mappers;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.NamesExtractor;

import java.io.IOException;
import java.util.List;

public class KeywordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text keywordText = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        CSVParser csvParser = new CSVParser();
        String[] tokens = csvParser.parseLine(value.toString());

        String keywordString = tokens[1];

        List<String> keywords = NamesExtractor.extractName(keywordString);

        for (String keyword : keywords) {
            keywordText.set(keyword);
            context.write(keywordText, one);
        }
    }
}
