package de.tub.dima.babelfish;

import org.openjdk.jmh.annotations.Benchmark;

public class HashmapBenchmark {

    @Benchmark
    public void init() {
        // Do nothing
    }

    public static void main(String[] args) throws Exception {
         org.openjdk.jmh.Main.main(args);
     }


}
