package de.tub.dima.babelfish.benchmark.layout;

import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkConfig;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.BenchmarkUtils;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.*;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;

import java.io.IOException;
import java.util.function.Function;

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeResultFile;

public class LayoutBenchmarkRunner extends AbstractBenchmark {

    private static int NUM_RECORDS = 10_000_000;

    private static Function<Integer, CachlineRecord> generator = new Function<Integer, CachlineRecord>() {
        @Override
        public CachlineRecord apply(Integer integer) {
            return new CachlineRecord(new EagerNumeric(integer, 0));
        }
    };

    public LayoutBenchmarkRunner(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        super();
        System.setProperty("ARROW_ENABLE_NULL_CHECK_FOR_GET", "false");
        System.setProperty("drill.enable_unsafe_memory_access", "true");
        System.setProperty("arrow.enable_unsafe_memory_access", "true");
        System.setProperty("arrow.enable_null_check_for_get", "false");
        BenchmarkConfig config = BenchmarkUtils.parseConfigLayout(args);
        LogicalOperator queryPlan = getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        RuntimeConfiguration.NAIVE_STRING_HANDLING = false;
        RuntimeConfiguration.LAZY_PARSING = config.lazy_string_handling;
        System.err.println("***");
        System.err.println("***");
        System.err.println("Start Benchmark: " + config.query + " in " + config.language + " with fields: " + config.fields + " Lazy Parsing: " + RuntimeConfiguration.LAZY_PARSING);
        System.err.println("***");
        System.err.println("***");
        BenchmarkResults results = executeBenchmark(queryPlan, 50, 100);
        writeResultFile("LayoutBenchmark", config, results);
    }

    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        new LayoutBenchmarkRunner(args);
    }

    LogicalOperator getQueryPlan(BenchmarkConfig config) throws IOException, SchemaExtractionException {

        switch (config.query) {
            case "row": {
                PhysicalLayoutFactory factory = new PhysicalLayoutFactory.RowLayoutFactory();
                BFLayoutGenerator.generate(NUM_RECORDS, generator, factory);
                return getBfQuery(config.fields);
            }
            case "column": {
                PhysicalLayoutFactory factory = new PhysicalLayoutFactory.ColumnLayoutFactory();
                BFLayoutGenerator.generate(NUM_RECORDS, generator, factory);
                return getBfQuery(config.fields);
            }
            case "csv": {
                CSVLayoutGenerator.generate(NUM_RECORDS, generator);
                return getCSVQuery(config.fields);
            }
            case "arrow": {
                ArrowLayoutGenerator.generate(NUM_RECORDS, generator);
                return getArrowQuery(config.fields);
            }
        }
        throw new RuntimeException("PredicationQuery: " + config.query + " is currently not supported!");
    }

    Sink getBfQuery(int numberOfFieldAccesses) {
        Scan scanCustomer = new Scan("layout");
        FieldReference[] fieldReferences = getProjections(numberOfFieldAccesses);
        Projection projectionCustomer = new Projection(fieldReferences);
        scanCustomer.addChild(projectionCustomer);
        Sink sink = new Sink.MemorySink();
        projectionCustomer.addChild(sink);
        return sink;
    }

    Sink getCSVQuery(int numberOfFieldAccesses) {
        CSVScan scanCustomer = new CSVScan("layout");
        FieldReference[] fieldReferences = getProjections(numberOfFieldAccesses);
        Projection projectionCustomer = new Projection(fieldReferences);
        scanCustomer.addChild(projectionCustomer);
        Sink sink = new Sink.MemorySink();
        projectionCustomer.addChild(sink);
        return sink;
    }

    Sink getArrowQuery(int numberOfFieldAccesses) {
        ArrowScan scanCustomer = new ArrowScan("layout");
        FieldReference[] fieldReferences = getProjections(numberOfFieldAccesses);
        Projection projectionCustomer = new Projection(fieldReferences);
        scanCustomer.addChild(projectionCustomer);
        Sink sink = new Sink.MemorySink();
        projectionCustomer.addChild(sink);
        return sink;
    }

    FieldReference[] getProjections(int size) {
        FieldReference[] projections = new FieldReference[size];
        for (int i = 0; i < size; i++) {
            projections[i] = new FieldReference("field_" + (i + 1), Int_32.class);
        }
        return projections;
    }

}
