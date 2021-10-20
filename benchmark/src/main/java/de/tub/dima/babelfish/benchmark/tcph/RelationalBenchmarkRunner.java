package de.tub.dima.babelfish.benchmark.tcph;

import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkConfig;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.BenchmarkUtils;
import de.tub.dima.babelfish.benchmark.parser.SSBImporter;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.benchmark.tcph.queries.*;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;

import java.io.IOException;

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeResultFile;

public class RelationalBenchmarkRunner extends AbstractBenchmark {

    public RelationalBenchmarkRunner(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        super();
        BenchmarkConfig config = BenchmarkUtils.parseConfig(args);
        LogicalOperator queryPlan = getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        RuntimeConfiguration.NAIVE_STRING_HANDLING = false;
        System.err.println("***");
        System.err.println("***");
        System.err.println("Start Benchmark: " + config.query + " in " + config.language);
        System.err.println("***");
        System.err.println("***");
        BenchmarkResults results = executeBenchmark(queryPlan, 150, 200);
        writeResultFile("RelBenchmark", config, results);
    }

    public static LogicalOperator getQueryPlan(BenchmarkConfig config) throws IOException, SchemaExtractionException {
        switch (config.query) {
            case "q1":
                TCPHImporter.importTCPH("/tpch/");
                return Query1.getExecution(config.language);
            case "q3":
                TCPHImporter.importTCPH("/tpch/");
                return Query3.getExecution(config.language);
            case "q6":
                TCPHImporter.importTCPH("/tpch/");
                return Query6.getExecution(config.language);
            case "q18":
                TCPHImporter.importTCPH("/tpch/");
                return Query18.getExecution(config.language);
            case "ssb11":
                SSBImporter.importSSB("/ssb/");
                return SSBQuery11.getExecution(config.language);
            case "ssb12":
                SSBImporter.importSSB("/ssb/");
                return SSBQuery12.getExecution(config.language);
            case "ssb13":
                SSBImporter.importSSB("/ssb/");
                return SSBQuery13.getExecution(config.language);
            case "ssb21":
                SSBImporter.importSSB("/ssb/");
                return SSBQuery21.getExecution(config.language);
            case "ssb23":
                SSBImporter.importSSB("/ssb/");
                return SSBQuery23.getExecution(config.language);
            case "ssb31":
                SSBImporter.importSSB("/ssb/");
                return SSBQuery31.getExecution(config.language);
            case "ssb41":
                SSBImporter.importSSB("/ssb/");
                return SSBQuery41.getExecution(config.language);

        }
        throw new RuntimeException("PredicationQuery: " + config.query + " is currently not supported!");
    }


    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        new RelationalBenchmarkRunner(args);
    }

}
