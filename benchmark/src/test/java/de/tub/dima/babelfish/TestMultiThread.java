package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;
import de.tub.dima.babelfish.ir.lqp.ParallelScan;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.*;
import de.tub.dima.babelfish.benchmark.parser.AirlineImporter;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import de.tub.dima.babelfish.BufferArgument;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class TestMultiThread {

    private BufferManager bufferManager;
    private Buffer buffer;
    @Before
    public void setup() throws IOException, SchemaExtractionException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");
        //TCPHImporter.importTCPH("/home/pgrulich/projects/luth-org/tpch-dbgen/");
        TCPHImporter.importTCPH("/home/pgrulich/projects/luth-org/tpch-10/");
        bufferManager = new BufferManager();
    }

    @Test
    public void simpleTest() throws IOException, SchemaExtractionException, InterruptedException {


        RuntimeConfiguration.LAZY_PARSING = true;
        RuntimeConfiguration.REPLACE_BY_ATOMIC = true;
        RuntimeConfiguration.REPLACE_BY_CAS_LOOP = true;
        ParallelScan scan = new ParallelScan("table.lineitem", 2, 10000);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("l_discount", Numeric.class, 2)));
        scan.addChild(groupBy);
        Sink sink = new Sink.PrintSink();
        groupBy.addChild(sink);

        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void simpleTestVintage() throws IOException, SchemaExtractionException, InterruptedException {


        RuntimeConfiguration.LAZY_PARSING = true;
        RuntimeConfiguration.MULTI_THREADED = true;
        Scan scan = new Scan("table.lineitem");

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("l_discount", Numeric.class, 2)));
        scan.addChild(groupBy);
        Sink sink = new Sink.PrintSink();
        groupBy.addChild(sink);

        executeQuery(new LogicalQueryPlan(sink));
    }


    @Test
    public void relationalQueryTCPH6() throws IOException, SchemaExtractionException, InterruptedException {

        RuntimeConfiguration.MULTI_THREADED = false;
        RuntimeConfiguration.REPLACE_BY_ATOMIC = true;
        RuntimeConfiguration.REPLACE_BY_CAS_LOOP = false;
        RuntimeConfiguration.REPLACE_WITH_PREAGGREGATION = false;

        RuntimeConfiguration.LAZY_PARSING = true;
        ParallelScan scan = new ParallelScan("table.lineitem", 2 , 10000);
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
        Sink sink = new Sink.PrintSink();
        groupBy.addChild(sink);

        executeQuery(new LogicalQueryPlan(sink));
    }


    @Test
    public void parallelEtl() throws IOException, SchemaExtractionException, InterruptedException {
        AirlineImporter.importAirlineData("/combined.csv");
        ParallelScan scan = new ParallelScan("table.airline", 2 , 10000);
        PythonOperator selection = new PythonOperator(new PythonUDF(
                "lambda rec,ctx: (not rec.Cancelled) and (rec.DepDelay >=10) and (rec.IATA_CODE_Reporting_Airline.equals(\"AA\") or rec.IATA_CODE_Reporting_Airline.equals(\"HA\"))"));
        scan.addChild(selection);
        Projection projection = new Projection(
                new FieldReference("IATA_CODE_Reporting_Airline", Text.class, 2),
                new FieldReference("Origin", Text.class, 6),
                new FieldReference("Dest", Text.class, 4),
                new FieldReference("DepDelay", Int_32.class),
                new FieldReference("ArrDelay", Int_32.class)
        );
        selection.addChild(projection);

        PythonOperator avgMap = new PythonOperator(new PythonUDF("def udf2(rec,ctx):\n" +
                "\trec.avgDelay = (rec.DepDelay + rec.ArrDelay) / 2 \n" +
                "\treturn rec\n" +
                "lambda a,ctx: udf2(a,ctx)"));
        projection.addChild(avgMap);
        PythonOperator delayMap = new PythonOperator(new PythonUDF("def udf3(rec,ctx):\n" +
                "\tif rec.avgDelay > 30:" +
                "\t\trec.delay = \"High\"\n" +
                "\telif rec.avgDelay < 20:\n" +
                "\t\trec.delay = \"Low\"\n" +
                "\telse:\n" +
                "\t\trec.delay = \"Medium\"\n" +
                "\treturn rec\n" +
                "lambda a,ctx: udf3(a,ctx)"));
        avgMap.addChild(delayMap);

        Projection projection2 = new Projection(
                new FieldReference("avgDelay", Float_64.class),
                new FieldReference("delay", Text.class, 6)
        );
        delayMap.addChild(projection2);
        Sink sink = new Sink.MemorySink();
        projection2.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }


    @Test
    public void simpletest() throws InterruptedException, IOException, SchemaExtractionException {
        executeQuery(new LogicalQueryPlan(new Sink()));
    }

    @Test
    public void relationalQueryTCPH1() throws IOException, SchemaExtractionException, InterruptedException {
        RuntimeConfiguration.MULTI_THREADED = true;
        RuntimeConfiguration.REPLACE_BY_ATOMIC = true;
        RuntimeConfiguration.REPLACE_BY_CAS_LOOP = false;

        RuntimeConfiguration.LAZY_PARSING = true;
        ParallelScan scan = new ParallelScan("table.lineitem", 1 , 100000);
        Selection selection = new Selection(new Predicate.LessThen<>(
                new FieldReference<>("l_shipdate", Date.class),
                new FieldConstant<>(new Date("1998-09-02"))));

        scan.addChild(selection);
        Function<?> function1 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice",  Numeric.class, 2)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1,2))),
                                new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class,2)),
                                Function.FunctionType.Min),
                        Function.FunctionType.Mul
                ), "disc_price");
        selection.addChild(function1);
        Function<?> function2 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("disc_price", Numeric.class, 4)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_tax", Numeric.class, 2)),
                                Function.FunctionType.Add),
                        Function.FunctionType.Mul
                ), "charge");
        function1.addChild(function2);
        GroupBy groupBy =
                new GroupBy(
                        new KeyGroup(
                                new FieldReference("l_returnflag", Char.class),
                                new FieldReference("l_linestatus", Char.class)),
                        new Aggregation.Count()
                );
        function2.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);


        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void relationalQueryTCPH18_single() throws IOException, SchemaExtractionException, InterruptedException {
        RuntimeConfiguration.MULTI_THREADED = false;
        RuntimeConfiguration.REPLACE_BY_ATOMIC = false;
        RuntimeConfiguration.REPLACE_BY_CAS_LOOP = false;

        RuntimeConfiguration.LAZY_PARSING = true;
        // scan lineitem and group by l_orderkey
        Scan scanLineitems = new Scan("table.lineitem");
        GroupBy groupByLineitem = new GroupBy(new KeyGroup(new FieldReference("l_orderkey", Int_32.class)),
                15000000,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class,2)));
        scanLineitems.addChild(groupByLineitem);


        Selection selectionGroupBy = new Selection(
                new Predicate.GreaterThan(
                        new FieldReference<>("sum_0", Numeric.class,2),
                        new FieldConstant<>(new EagerNumeric(30000,2))
                )
        );
        groupByLineitem.addChild(selectionGroupBy);

        Sink sink = new Sink.MemorySink();
        selectionGroupBy.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void relationalQueryTCPH18() throws IOException, SchemaExtractionException, InterruptedException {
        RuntimeConfiguration.MULTI_THREADED = true;
        RuntimeConfiguration.REPLACE_BY_ATOMIC = true;
        RuntimeConfiguration.REPLACE_BY_CAS_LOOP = false;

        RuntimeConfiguration.LAZY_PARSING = true;
        // scan lineitem and group by l_orderkey
        ParallelScan scanLineitems = new ParallelScan("table.lineitem",1, 100000);
        GroupBy groupByLineitem = new GroupBy(new KeyGroup(new FieldReference("l_orderkey", Int_32.class)),
                1500000,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class,2)));
        scanLineitems.addChild(groupByLineitem);


        Selection selectionGroupBy = new Selection(
                new Predicate.GreaterThan(
                        new FieldReference<>("sum_0", Numeric.class,2),
                        new FieldConstant<>(new EagerNumeric(30000,2))
                )
        );
        groupByLineitem.addChild(selectionGroupBy);

        Sink sink = new Sink.MemorySink();
        selectionGroupBy.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    public void executeQuery(LogicalQueryPlan plan) throws IOException, InterruptedException, SchemaExtractionException {

        Thread.sleep(1000);
        Value executableQuery = submitQuery(plan);
        Thread.sleep(1000);
        for (int i = 0; i < 20000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, bufferManager));
            System.out.println("Execution Time:" + time);
            //if (time.asLong() == 0) {
            //    Thread.sleep(10000);
             //   return;
           // }
            Thread.sleep(10);
        }
        Assert.fail();
    }

    static Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth", "js", "python")
                    .allowCreateThread(true)
                    // .option("inspect","true")
                    // org/graalvm/compiler/truffle/options/PolyglotCompilerOptions.java
                    //.option("engine.Inlining", "false")
                     //  .option("engine.TracePerformanceWarnings", "true")
                    .option("engine.TraceCompilation", "true")
                    .option("engine.TraceCompilationDetails", "true")
                    .option("engine.CompilationExceptionsArePrinted", "true")
                    //  .option("engine.OSRCompilationThreshold", "10000")
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
