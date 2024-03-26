package hadoop.mapreduce;

public enum OperationTypes {
    AVG_RATE_BY_GENRE("rev_genre"),
    AVG_RATE_BY_COMPANY("rev_company"),
    DIST_BY_GENRE("dist_genre"),
    KEYWORD_COUNT("keyword_count");

    private final String value;

    OperationTypes(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
