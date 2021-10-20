package de.tub.dima.babelfish.benchmark.parallel;

import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkConfig;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.BenchmarkUtils;
import de.tub.dima.babelfish.benchmark.datatypes.Lineitem_row;
import de.tub.dima.babelfish.benchmark.parallel.queries.Agg;
import de.tub.dima.babelfish.benchmark.parallel.queries.Query1;
import de.tub.dima.babelfish.benchmark.parallel.queries.Query6;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeResultFile;
import static de.tub.dima.babelfish.benchmark.parser.SSBImporter.importTable;

public class MultiThreadBenchmarkRunner extends AbstractBenchmark {

    public MultiThreadBenchmarkRunner(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        super();
        BenchmarkConfig config = BenchmarkUtils.parseConfigLayout(args);
        LogicalOperator queryPlan = getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        RuntimeConfiguration.NAIVE_STRING_HANDLING = false;
        RuntimeConfiguration.MULTI_THREADED = false;
        RuntimeConfiguration.REPLACE_BY_ATOMIC = config.lazy_string_handling;
        RuntimeConfiguration.REPLACE_BY_CAS_LOOP = config.lazy_string_handling;
        System.err.println("***");
        System.err.println("***");
        System.err.println("Start Benchmark: " + config.query + " in " + config.language);
        System.err.println("***");
        System.err.println("***");
        BenchmarkResults results = executeBenchmark(queryPlan, 100, 200);
        writeResultFile("MultiThreadBenchmark", config, results);
    }

    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        new MultiThreadBenchmarkRunner(args);
    }

    LogicalOperator getQueryPlan(BenchmarkConfig config) throws IOException, SchemaExtractionException {
        switch (config.query) {
            case "q6":
                TCPHImporter.importTCPH("/tpch/");
                return Query6.getExecution(config.language, config.fields);
            case "q6k": {

                PhysicalLayoutFactory factory = new PhysicalLayoutFactory.ColumnLayoutFactory();
                BufferManager bufferManager = new BufferManager();
                AtomicInteger counter = new AtomicInteger(0);
                importTable(bufferManager, "/tpch/" + "lineitem.tbl", "table.lineitem_row", Lineitem_row.class, (List<String> fields) -> new Lineitem_row(
                        counter.getAndIncrement() % config.fields,
                        Integer.valueOf(fields.get(0)),
                        Integer.valueOf(fields.get(1)),
                        Integer.valueOf(fields.get(2)),
                        Integer.valueOf(fields.get(3)),
                        new EagerNumeric((long) (Float.valueOf(fields.get(4)) * 100), 2),
                        new EagerNumeric((long) (Float.valueOf(fields.get(5)) * 100), 2),
                        new EagerNumeric((long) (Float.valueOf(fields.get(6)) * 100), 2),
                        new EagerNumeric((long) (Float.valueOf(fields.get(7)) * 100), 2),
                        new Char(fields.get(8).toCharArray()[0]),
                        new Char(fields.get(9).toCharArray()[0]),
                        new Date(fields.get(10)),
                        new Date(fields.get(11)),
                        new Date(fields.get(12))
                ), factory);
                return Query6.relationalQueryTCPH6_keyed(24);
            }
            case "q1":
                TCPHImporter.importTCPH("/tpch/");
                return Query1.relationalQueryTCPH1(config.fields);
            case "q18":
                TCPHImporter.importTCPH("/tpch/");
                return Query1.relationalQueryTCPH18(config.fields);
            case "add":
                TCPHImporter.importTCPH("/tpch/");
                return Agg.add(config.fields);
            case "min":
                TCPHImporter.importTCPH("/tpch/");
                return Agg.min(config.fields);

        }
        throw new RuntimeException("PredicationQuery: " + config.query + " is currently not supported!");
    }

}
