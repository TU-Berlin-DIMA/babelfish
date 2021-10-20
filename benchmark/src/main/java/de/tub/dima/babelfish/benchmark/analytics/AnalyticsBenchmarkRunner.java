package de.tub.dima.babelfish.benchmark.analytics;

import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkConfig;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.BenchmarkUtils;
import de.tub.dima.babelfish.benchmark.analytics.queries.ETLQuery;
import de.tub.dima.babelfish.benchmark.analytics.queries.MaxDelay;
import de.tub.dima.babelfish.benchmark.analytics.queries.TwoGram;
import de.tub.dima.babelfish.benchmark.analytics.queries.WordCount;
import de.tub.dima.babelfish.benchmark.parser.AirlineImporter;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;

import java.io.IOException;

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeResultFile;

public class AnalyticsBenchmarkRunner extends AbstractBenchmark {

    public AnalyticsBenchmarkRunner(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        super();

        BenchmarkConfig config = BenchmarkUtils.parseConfig(args);
        LogicalOperator queryPlan = getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        BenchmarkResults results = executeBenchmark(queryPlan, 100, 100);
        writeResultFile("AnalyticsBenchmark", config, results);
    }

    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        new AnalyticsBenchmarkRunner(args);
    }

    LogicalOperator getQueryPlan(BenchmarkConfig config) throws IOException, SchemaExtractionException {
        switch (config.query) {
            case "etl": {
                AirlineImporter.importAirlineData("/combined.csv");
                return ETLQuery.getExecution(config.language);
            }
            case "delay": {
                AirlineImporter.importAirlineData("/combined.csv");
                return MaxDelay.getExecution(config.language);
            }
            case "wordcount": {
                TCPHImporter.importTCPH("/tpch/");
                return WordCount.getExecution(config.language);
            }
            case "twogram": {
                TCPHImporter.importTCPH("/tpch/");
                return TwoGram.getExecution(config.language);
            }
        }
        throw new RuntimeException("PredicationQuery: " + config.query + " is currently not supported!");
    }
}
