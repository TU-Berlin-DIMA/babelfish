package de.tub.dima.babelfish.benchmark.string;

import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkConfig;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.BenchmarkUtils;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.benchmark.string.queries.*;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;

import java.io.IOException;

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeResultFile;

public class StringBenchmarkRunner extends AbstractBenchmark {

    public StringBenchmarkRunner(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        super();
        TCPHImporter.importTCPH("/home/pgrulich/projects/luth-org/tpch-dbgen/");
        BenchmarkConfig config = BenchmarkUtils.parseConfig(args);
        LogicalOperator queryPlan = getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = config.lazy_string_handling;
        System.err.println("***");
        System.err.println("***");
        System.err.println("Start Benchmark: " + config.query + " in " + config.language + " With LazyText=" + config.lazy_string_handling);
        System.err.println("***");
        System.err.println("***");
        executeBenchmark(queryPlan, 0, 200);
        BenchmarkResults results = executeBenchmark(queryPlan, 0, 200);
        writeResultFile("StringBenchmark", config, results);
    }

    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        new StringBenchmarkRunner(args);
    }

    LogicalOperator getQueryPlan(BenchmarkConfig config) {
        switch (config.query) {
            case "equals":
                return StringEquals.getExecution(config.language);
            case "equals_naive":
                return StringEqualsNative.getExecution(config.language);
            case "copy":
                return StringCopy.getExecution(config.language);
            case "copy_naive":
                return StringCopyNative.getExecution(config.language);
            case "lowercase":
                return StringLowercase.getExecution(config.language);
            case "lowercase_naive":
                return StringLowercaseNaive.getExecution(config.language);
            case "uppercase":
                return StringUppercase.getExecution(config.language);
            case "uppercase_naive":
                return StringUppercaseNaive.getExecution(config.language);
            case "substring":
                return StringSubstring.getExecution(config.language);
            case "substring_naive":
                return StringSubstringNaive.getExecution(config.language);
            case "reverse":
                return StringReverse.getExecution(config.language);
            case "reverse_naive":
                return StringReverseNaive.getExecution(config.language);
            case "concat":
                return StringConcat.getExecution(config.language);
            case "concat_naive":
                return StringConcatNaive.getExecution(config.language);
            case "split":
                return StringSplit.getExecution(config.language);
            case "split_naive":
                return StringSplitNaive.getExecution(config.language);
        }
        throw new RuntimeException("PredicationQuery: " + config.query + " is currently not supported!");
    }


}
