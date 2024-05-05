package hadoop.mapreduce;

public enum OperationTypes {
    AVG_RATE_BY_GENRE("avg_rate_by_genre"),
    AVG_RATE_BY_COMPANY("avg_rate_by_company"),
    DIST_BY_GENRE("dist_by_genre"),
    KEYWORD_COUNT("keyword_count");

    private final String value;

    OperationTypes(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
