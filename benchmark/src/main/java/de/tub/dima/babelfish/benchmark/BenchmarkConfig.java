package de.tub.dima.babelfish.benchmark;

public class BenchmarkConfig {
    public final String query;
    public final String language;
    public final boolean lazy_string_handling;
    public final int fields;


    public BenchmarkConfig(String query, String language, boolean lazy_string_handling) {
        this.query = query;
        this.language = language;
        this.lazy_string_handling = lazy_string_handling;
        fields = 0;
    }

    public BenchmarkConfig(String query, String language, int fields, boolean lazy_string_handling) {
        this.query = query;
        this.language = language;
        this.lazy_string_handling = lazy_string_handling;
        this.fields = fields;
    }
}
