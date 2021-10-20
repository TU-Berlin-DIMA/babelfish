package de.tub.dima.babelfish.benchmark.predication;

import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkConfig;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.BenchmarkUtils;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;

import java.io.IOException;

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeResultFile;

public class PredicationBenchmarkRunner extends AbstractBenchmark {

    public PredicationBenchmarkRunner(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        super();
        BenchmarkConfig config = BenchmarkUtils.parseConfig(args);
        LogicalOperator queryPlan = getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        RuntimeConfiguration.NAIVE_STRING_HANDLING = false;
        RuntimeConfiguration.ELIMINATE_FILTER_IF = config.lazy_string_handling;
        RuntimeConfiguration.ELIMINATE_EMPTY_IF = config.lazy_string_handling;
        RuntimeConfiguration.ELIMINATE_EMPTY_IF_PROFILING = true;
        System.err.println("***");
        System.err.println("***");
        System.err.println("Start Benchmark: " + config.query + " in " + config.language + " predication: " + config.lazy_string_handling);
        System.err.println("***");
        System.err.println("***");
        BenchmarkResults results = executeBenchmark(queryPlan, 150, 200);
        writeResultFile("PredicationBenchmark", config, results);
    }

    public static LogicalOperator getQueryPlan(BenchmarkConfig config) throws IOException, SchemaExtractionException {
        // value for 10gb of data
        long size = 125000000L;
        PredicationQuery.createData(size);
        switch (config.language) {
            case "rel":
                return PredicationQuery.relSelection(Integer.parseInt(config.query));
            case "js":
                return PredicationQuery.javaScriptSelection(Integer.parseInt(config.query));
            case "python":
                return PredicationQuery.pythonSelection(Integer.parseInt(config.query));
            case "java":
                return PredicationQuery.javaSelection(Integer.parseInt(config.query));
        }

        return null;

    }


    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        new PredicationBenchmarkRunner(args);
    }

}
