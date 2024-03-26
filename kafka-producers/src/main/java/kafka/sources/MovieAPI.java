package kafka.sources;

import org.json.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class MovieAPI implements APISource<JSONObject>{
    private static final String API_KEY = "2d3570e4377a0aefdc8f97df1b51ac88";

    public static JSONObject getMovie() throws IOException {
        HttpClient httpClient = HttpClients.createDefault();
        int page = ThreadLocalRandom.current().nextInt(1, 500); // Generate a random page number between 1 and 2
        String url = "https://api.themoviedb.org/3/movie/popular?language=en-US&page=" + page +
                "&api_key="+API_KEY;
        HttpGet httpGet = new HttpGet(url);

        String responseBody = EntityUtils.toString(httpClient.execute(httpGet).getEntity());

        JSONObject jsonObject = new JSONObject(responseBody);
        JSONArray results = jsonObject.getJSONArray("results");

        int randomIndex = ThreadLocalRandom.current().nextInt(0, results.length());
        return results.getJSONObject(randomIndex);
    }

    @Override
    public JSONObject getResponse() throws IOException {
        return  getMovie();
    }

}
