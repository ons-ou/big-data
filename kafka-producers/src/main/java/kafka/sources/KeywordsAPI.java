package kafka.sources;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

public class KeywordsAPI implements APISource<JSONArray>{

    private static final String API_KEY = "2d3570e4377a0aefdc8f97df1b51ac88";

    @Override
    public JSONArray getResponse() throws IOException {
        JSONObject movie = MovieAPI.getMovie();
        HttpClient httpClient = HttpClients.createDefault();

        String url = "https://api.themoviedb.org/3/movie/" + movie.getInt("id") + "/keywords?api_key=" + API_KEY;
        HttpGet httpGet = new HttpGet(url);
        String responseBody = EntityUtils.toString(httpClient.execute(httpGet).getEntity());

        JSONObject jsonObject = new JSONObject(responseBody);
        return jsonObject.getJSONArray("keywords");

    }
}
