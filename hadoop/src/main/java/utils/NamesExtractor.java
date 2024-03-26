package utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.List;

public class NamesExtractor {

    public static List<String> extractName(String genresJson) {
        List<String> genreNames = new ArrayList<>();
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        JsonArray genresArray = parser.parse(genresJson).getAsJsonArray();

        for (JsonElement genreElement : genresArray) {
            String genreName = genreElement.getAsJsonObject().get("name").getAsString();
            genreNames.add(genreName);
        }

        return genreNames;
    }
}