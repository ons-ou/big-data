package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class YearExtractor {

    public static int extractYear(String dateString) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(dateString);
            return date.getYear() + 1900; // Date.getYear() returns years since 1900
        } catch (ParseException e) {
            e.printStackTrace();
            return -1; // Return -1 if parsing fails
        }
    }
}