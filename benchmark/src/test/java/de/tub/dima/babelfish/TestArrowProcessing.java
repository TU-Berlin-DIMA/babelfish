package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.ir.lqp.ArrowScan;
import de.tub.dima.babelfish.ir.lqp.CSVScan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.*;
import de.tub.dima.babelfish.benchmark.parser.ArrowCSVImporter;
import de.tub.dima.babelfish.benchmark.parser.TCPCSVImporter;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import de.tub.dima.babelfish.BufferArgument;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;

public class TestArrowProcessing {


    private Buffer buffer;
    private BufferManager bufferManager;

    @Before
    public void setup() throws IOException, SchemaExtractionException {
        System.setProperty("ARROW_ENABLE_NULL_CHECK_FOR_GET", "false");
        System.setProperty("drill.enable_unsafe_memory_access", "true");
        System.setProperty("arrow.enable_unsafe_memory_access", "true");
        System.setProperty("arrow.enable_null_check_for_get", "false");
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");
        System.setProperty("python.home", ".");
        bufferManager = new BufferManager();
    }
    @Test
    public void relationalQueryArrow() throws IOException, SchemaExtractionException, InterruptedException {

        RootAllocator allocator = new RootAllocator();

        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
// create dictionary and provider

        IntVector dataVector = new IntVector("test", allocator);
        for(int i = 0 ; i< 10000000;i++){
            dataVector.setSafe(i,42);
        }
        dataVector.setValueCount(10000000);



// create VectorSchemaRoot
        List<Field> fields = Arrays.asList(dataVector.getField());
        List<FieldVector> vectors = Arrays.asList(dataVector);
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

// write data
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, provider, Channels.newChannel(out));
        writer.start();
        writer.writeBatch();
        writer.end();

// read data
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();

            System.out.println(readRoot.getRowCount());

            // get the encoded vector
            IntVector intVector = (IntVector) readRoot.getVector(0);

            System.out.println(intVector.get(0));
            System.out.println(intVector.get(1));
            System.out.println(intVector.get(2));

        }
    }

    @Test
    public void relationalQueryTCPH6() throws IOException, SchemaExtractionException, InterruptedException {
        RootAllocator allocator = new RootAllocator();
        ArrowCSVImporter.importArrow("/tpch/", allocator);
        RuntimeConfiguration.LAZY_PARSING = true;
        ArrowScan scan = new ArrowScan("table.lineitem");
        Selection selection = new Selection(
                new Predicate.And(
                        new Predicate.GreaterEquals<>(
                                new FieldReference<>("l_shipdate", Date.class),
                                new FieldConstant<>(new Date("1994-01-01"))),
                        new Predicate.And(
                                new Predicate.LessThen<>(
                                        new FieldReference<>("l_shipdate", Date.class),
                                        new FieldConstant<>(new Date("1995-01-01"))),
                                new Predicate.And(
                                        new Predicate.LessEquals<>(
                                                new FieldReference<>("l_discount", Numeric.class, 2),
                                                new FieldConstant<>(new EagerNumeric(7, 2))),
                                        new Predicate.And(
                                                new Predicate.LessThen<>(
                                                        new FieldReference<>("l_quantity", Numeric.class, 2),
                                                        new FieldConstant<>(new EagerNumeric(2400, 2))),
                                                new Predicate.GreaterEquals<>(
                                                        new FieldReference<>("l_discount", Numeric.class, 2),
                                                        new FieldConstant<>(new EagerNumeric(5,2)))
                                        )
                                )
                        )
                ));
        scan.addChild(selection);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class,2)),
                        Function.FunctionType.Mul
                ), "revenue");
        selection.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 2)));
        function.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        executeQuery(new LogicalQueryPlan(sink));
    }


    @Test
    public void csvQuery6() throws IOException, InterruptedException, SchemaExtractionException {
        TCPCSVImporter.importTCPH("/tpch/tpch-dbgen/");
        CSVScan scan = new CSVScan("table.lineitem");
        PythonOperator selection = new PythonOperator(new PythonUDF(
                "lambda rec,ctx:   (rec.l_discount > 0.05) & (rec.l_discount < 0.07) "));
        scan.addChild(selection);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class,2)),
                        Function.FunctionType.Mul
                ), "revenue");
        selection.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 2)));
        function.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void stringSubstringPython() throws IOException, InterruptedException, SchemaExtractionException {
        TCPCSVImporter.importTCPH("/tpch/tpch-dbgen/");
        CSVScan scan = new CSVScan("table.lineitem");
        PythonOperator selection = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\treturn(rec.l_discount*42,42)" +
                "\nlambda rec,ctx: udf(rec,ctx)"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }


    public void executeQuery(LogicalQueryPlan plan) throws IOException, InterruptedException, SchemaExtractionException {

        Thread.sleep(1000);
        Value executableQuery = submitQuery(plan);
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, bufferManager));
            System.out.println("Execution Time:" + time);
            if (time.asLong() == 0) {
                Thread.sleep(10000);
                return;
            }
            Thread.sleep(100);
        }
        Assert.fail();
    }

    static Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth", "js", "python")
                    // .option("inspect","true")
                    // org/graalvm/compiler/truffle/options/PolyglotCompilerOptions.java
                    //.option("engine.Inlining", "false")
                    //   .option("engine.TracePerformanceWarnings", "true")
                    //  .option("engine.TruffleCompilation", "false")
                    .option("engine.IterativePartialEscape", "true")
                    //.option("engine.EscapeAnalysisIterations", "10")
                    //  .option("engine.CompileImmediately", "true")
                    .allowAllAccess(true).build();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(plan);
            oos.flush();
            oos.close();
            Source s = Source.newBuilder("luth", ByteSequence.create(baos.toByteArray()), "testPlan").build();
            Value pipeline = context.eval(s);
            return pipeline;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
