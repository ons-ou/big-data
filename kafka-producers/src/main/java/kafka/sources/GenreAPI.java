package kafka.sources;

import org.json.JSONArray;
import org.json.JSONObject;
import utils.GenresList;

import java.io.IOException;

public class GenreAPI implements APISource<JSONArray> {

    @Override
    public JSONArray getResponse() throws IOException {
        JSONObject movie = MovieAPI.getMovie();
        JSONArray genreIds = movie.getJSONArray("genre_ids");

        return GenresList.getMovieGenres(genreIds);
    }
}
