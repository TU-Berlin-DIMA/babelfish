package de.tub.dima.babelfish.benchmark.externalLibs;

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

public class ExternalLibBenchmarkRunner extends AbstractBenchmark {

    public ExternalLibBenchmarkRunner(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        super();

        BenchmarkConfig config = BenchmarkUtils.parseConfig(args);
        LogicalOperator queryPlan = getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        BenchmarkResults results = executeBenchmark(queryPlan, 100, 100);
        writeResultFile("ExternalLibBenchmark", config, results);
    }

    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        new ExternalLibBenchmarkRunner(args);
    }

    LogicalOperator getQueryPlan(BenchmarkConfig config) throws IOException, SchemaExtractionException {
        switch (config.query) {
            case "cleaning": {
                return Queries.getReQueries();
            }
            case "time": {
                return Queries.getArrowQuery();
            }
            case "distance": {
                return Queries.getDistanceFunction();
            }
            case "linearRegression": {
                return Queries.getLinearRegression();
            }
            case "blackSholes": {
                return Queries.getBlackScholes();
            }
            case "crimeIndex": {
                return Queries.getCrimeIndex();
            }
            case "airlineEtl": {
                return Queries.getAirlinesEtl();
            }
        }
        throw new RuntimeException("ExternalLibBenchmark: " + config.query + " is currently not supported!");
    }
}
