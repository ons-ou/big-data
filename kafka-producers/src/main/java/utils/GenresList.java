package utils;

import org.json.JSONArray;
import org.json.JSONObject;

public class GenresList {

    public static JSONArray getGenresList(){
        return new JSONArray()
                .put(new JSONObject().put("id", 28).put("name", "Action"))
                .put(new JSONObject().put("id", 12).put("name", "Adventure"))
                .put(new JSONObject().put("id", 16).put("name", "Animation"))
                .put(new JSONObject().put("id", 35).put("name", "Comedy"))
                .put(new JSONObject().put("id", 80).put("name", "Crime"))
                .put(new JSONObject().put("id", 99).put("name", "Documentary"))
                .put(new JSONObject().put("id", 18).put("name", "Drama"))
                .put(new JSONObject().put("id", 10751).put("name", "Family"))
                .put(new JSONObject().put("id", 14).put("name", "Fantasy"))
                .put(new JSONObject().put("id", 36).put("name", "History"))
                .put(new JSONObject().put("id", 27).put("name", "Horror"))
                .put(new JSONObject().put("id", 10402).put("name", "Music"))
                .put(new JSONObject().put("id", 9648).put("name", "Mystery"))
                .put(new JSONObject().put("id", 10749).put("name", "Romance"))
                .put(new JSONObject().put("id", 878).put("name", "Science Fiction"))
                .put(new JSONObject().put("id", 10770).put("name", "TV Movie"))
                .put(new JSONObject().put("id", 53).put("name", "Thriller"))
                .put(new JSONObject().put("id", 10752).put("name", "War"))
                .put(new JSONObject().put("id", 37).put("name", "Western"));
    }

    public static JSONArray getMovieGenres(JSONArray genreIds){
        JSONArray movieGenres = new JSONArray();
        JSONArray genres = GenresList.getGenresList();

        for (int i = 0; i < genreIds.length(); i++) {
            int id = genreIds.getInt(i);
            for (int j = 0; j < genres.length(); j++) {
                JSONObject genre = genres.getJSONObject(j);
                if (genre.getInt("id") == id) {
                    movieGenres.put(genre);
                    break;
                }
            }
        }
        return movieGenres;
    }
}
