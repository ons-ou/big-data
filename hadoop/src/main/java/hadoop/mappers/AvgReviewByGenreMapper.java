package hadoop.mappers;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.NamesExtractor;

import java.io.IOException;
import java.util.List;

public class AvgReviewByGenreMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private FloatWritable review = new FloatWritable();
    private Text genreValue = new Text();

    public void map(Object key, Text value, Context context
    ) {
        CSVParser csvParser = new CSVParser();
        try {
            String[] tokens = csvParser.parseLine(value.toString());
            assert tokens.length == 23;

            String genres = tokens[3];
            float avgReview = Float.parseFloat(tokens[22]);

            List<String> genreNames = NamesExtractor.extractName(genres);

            review.set(avgReview);
            for (String genre : genreNames) {
                genreValue.set(genre);
                System.out.println(genre + " " + review.get());
                context.write(genreValue, review);
            }
        } catch (Exception ex){}

    }
}
