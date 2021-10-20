package de.tub.dima.babelfish.benchmark;

import java.util.ArrayList;
import java.util.List;

public class BenchmarkResults {

    public final List<BenchmarkResult> results = new ArrayList<>();

    public void add(BenchmarkResult result) {
        results.add(result);
    }

    public BenchmarkStatistics getInternal() {
        long min = Long.MAX_VALUE;
        long max = 0;
        long sum = 0;
        for (BenchmarkResult result : results) {
            long time = result.internalTime;
            min = Math.min(time, min);
            max = Math.max(time, max);
            sum = sum + time;
        }
        return new BenchmarkStatistics(min, max, sum / (1.0 * results.size()));
    }

    public BenchmarkStatistics getExternal() {
        long min = Long.MAX_VALUE;
        long max = 0;
        long sum = 0;
        for (BenchmarkResult result : results) {
            long time = result.externalTime;
            min = Math.min(time, min);
            max = Math.max(time, max);
            sum = sum + time;
        }
        return new BenchmarkStatistics(min, max, sum / (1.0 * results.size()));
    }

    public static class BenchmarkStatistics {
        public final long min;
        public final long max;
        public final double avg;

        public BenchmarkStatistics(long min, long max, double avg) {
            this.min = min;
            this.max = max;
            this.avg = avg;
        }
    }

    public static class BenchmarkResult {
        public final long internalTime;
        public final long externalTime;

        public BenchmarkResult(long internalTime, long externalTime) {
            this.internalTime = internalTime;
            this.externalTime = externalTime;
        }
    }
}
