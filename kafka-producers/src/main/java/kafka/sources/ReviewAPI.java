package kafka.sources;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class ReviewAPI implements APISource<JSONObject>{

    private static final String API_KEY = "2d3570e4377a0aefdc8f97df1b51ac88";

    @Override
    public JSONObject getResponse() throws IOException {
        JSONObject movie = MovieAPI.getMovie();
        HttpClient httpClient = HttpClients.createDefault();
        String url = "https://api.themoviedb.org/3/movie/" + movie.getInt("id") + "/reviews?language=en-US&page=1&api_key=" + API_KEY;
        HttpGet httpGet = new HttpGet(url);

        JSONArray results = new JSONArray();
        JSONObject jsonObject;
        while (results.isEmpty() || !hasRating(results)) {
            String responseBody = EntityUtils.toString(httpClient.execute(httpGet).getEntity());
            jsonObject = new JSONObject(responseBody);
            results = jsonObject.getJSONArray("results");
        }

        int randomIndex = ThreadLocalRandom.current().nextInt(0, results.length());
        jsonObject = results.getJSONObject(randomIndex);
        jsonObject.append("movie", movie);
        return jsonObject;
    }

    private boolean hasRating(JSONArray results) {
        for (int i = 0; i < results.length(); i++) {
            JSONObject review = results.getJSONObject(i);
            if (review.has("author_details") && !review.isNull("author_details")) {
                JSONObject authorDetails = review.getJSONObject("author_details");
                if (authorDetails.has("rating") && !authorDetails.isNull("rating")) {
                    return true;
                }
            }
        }
        return false;
    }
}
