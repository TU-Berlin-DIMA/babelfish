package de.tub.dima.babelfish.benchmark.datasources;

import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkConfig;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.BenchmarkUtils;
import de.tub.dima.babelfish.benchmark.datasources.queries.DataSourceQuery3;
import de.tub.dima.babelfish.benchmark.datasources.queries.DataSourceQuery6;
import de.tub.dima.babelfish.benchmark.parser.ArrowCSVImporter;
import de.tub.dima.babelfish.benchmark.parser.TCPCSVImporter;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.benchmark.tcph.queries.Query3;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.ArrowScan;
import de.tub.dima.babelfish.ir.lqp.CSVScan;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeResultFile;

public class DataSourceBenchmarkRunner extends AbstractBenchmark {

    public DataSourceBenchmarkRunner(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        super();
        System.setProperty("ARROW_ENABLE_NULL_CHECK_FOR_GET", "false");
        System.setProperty("drill.enable_unsafe_memory_access", "true");
        System.setProperty("arrow.enable_unsafe_memory_access", "true");
        System.setProperty("arrow.enable_null_check_for_get", "false");
        BenchmarkConfig config = BenchmarkUtils.parseConfig(args);
        LogicalOperator queryPlan = getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        RuntimeConfiguration.NAIVE_STRING_HANDLING = false;
        RuntimeConfiguration.LAZY_PARSING = config.lazy_string_handling;
        System.err.println("***");
        System.err.println("***");
        System.err.println("Start Benchmark: " + config.query + " in " + config.language + " " + config.lazy_string_handling);
        System.err.println("***");
        System.err.println("***");
        BenchmarkResults results = executeBenchmark(queryPlan, 50, 100);
        writeResultFile("DataSourceBenchmark", config, results);
    }

    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        new DataSourceBenchmarkRunner(args);
    }

    LogicalOperator getQueryPlan(BenchmarkConfig config) throws IOException, SchemaExtractionException, InterruptedException {
        switch (config.query) {
            case "q6_row": {
                PhysicalLayoutFactory factory = new PhysicalLayoutFactory.RowLayoutFactory();
                TCPHImporter.importTCPH("/tpch/", factory);
                return DataSourceQuery6.relationalQueryTCPH6_Arrow(new Scan("table.lineitem"));
            }
            case "q6_column": {
                PhysicalLayoutFactory factory = new PhysicalLayoutFactory.ColumnLayoutFactory();
                TCPHImporter.importTCPH("/tpch/", factory);
                return DataSourceQuery6.relationalQueryTCPH6_Arrow(new Scan("table.lineitem"));
            }
            case "q6_csv": {
                TCPCSVImporter.importTCPH("/tpch/");
                return DataSourceQuery6.relationalQueryTCPH6_csv(new CSVScan("table.lineitem"));
            }
            case "q6_arrow": {
                RootAllocator allocator = new RootAllocator();
                ArrowCSVImporter.importArrow("/tpch/", allocator);
                return DataSourceQuery6.relationalQueryTCPH6_Arrow(new ArrowScan("table.lineitem"));
            }
            case "q3_row": {
                PhysicalLayoutFactory factory = new PhysicalLayoutFactory.RowLayoutFactory();
                TCPHImporter.importTCPH("/tpch/", factory);
                return Query3.relationalQuery3();
            }
            case "q3_column": {
                PhysicalLayoutFactory factory = new PhysicalLayoutFactory.ColumnLayoutFactory();
                TCPHImporter.importTCPH("/tpch/", factory);
                return Query3.relationalQuery3();
            }
            case "q3_csv": {
                TCPCSVImporter.importTCPH("/tpch/");
                return DataSourceQuery3.relationalQuery3(
                        new CSVScan("table.customer"),
                        new CSVScan("table.orders"),
                        new CSVScan("table.lineitem"));
            }
            case "q3_arrow": {
                RootAllocator allocator = new RootAllocator();
                ArrowCSVImporter.importArrow("/tpch/", allocator);
                return DataSourceQuery3.relationalQuery3(
                        new ArrowScan("table.customer"),
                        new ArrowScan("table.orders"),
                        new ArrowScan("table.lineitem"));
            }

        }
        throw new RuntimeException("PredicationQuery: " + config.query + " is currently not supported!");
    }

}
