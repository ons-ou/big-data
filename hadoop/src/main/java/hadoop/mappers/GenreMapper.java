package hadoop.mappers;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.NamesExtractor;
import utils.YearExtractor;

import java.io.IOException;
import java.util.List;

public class GenreMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text genreYearPair = new Text();

    public void map(Object key, Text value, Context context
    ) {
        CSVParser csvParser = new CSVParser();
        try{
            String[] tokens = csvParser.parseLine(value.toString());
            assert tokens.length == 23;
            String genres = tokens[3];
            int year = YearExtractor.extractYear(tokens[14]);

            List<String> genreNames = NamesExtractor.extractName(genres);

            for (String genre : genreNames) {
                genreYearPair.set(genre + "," + year);
                context.write(genreYearPair, one);
            }
        } catch (Exception exception){

        }

    }
}
