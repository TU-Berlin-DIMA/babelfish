package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.typesytem.record.*;
import de.tub.dima.babelfish.typesytem.udt.CSVSourceDate;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.CSVSourceNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.ir.lqp.CSVScan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.*;
import de.tub.dima.babelfish.benchmark.parser.TCPCSVImporter;
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

public class TestCSVProcessing {


    private Buffer buffer;
    private BufferManager bufferManager;


    @Before
    public void setup() throws IOException, SchemaExtractionException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");
        System.setProperty("python.home", ".");
        bufferManager = new BufferManager();
    }

    @Test
    public void parseDateTest() {
        long mem = UnsafeUtils.allocateMemory(new Unit.Bytes(10)).getAddress();

        String name = "1994-02-24";
        for (int i = 0; i < 10; i++) {
            long address = mem + i;
            System.out.println(address);
            UnsafeUtils.putByte(mem+i, (byte)name.charAt(i));
        }

        CSVSourceDate c = new CSVSourceDate(mem, mem + 10);
        Assert.assertEquals(19940224, c.getUnixTs());
        Assert.assertEquals(19940224, (int)Date.parse("1994-02-24"));
    }

    @Test
    public void parseCharTest() {
        long mem = UnsafeUtils.allocateMemory(new Unit.Bytes(10)).getAddress();

        String name = "1010.42";
        for (int i = 0; i < 7; i++) {
            long address = mem + i;
            System.out.println(address);
            UnsafeUtils.putByte(mem+i, (byte)name.charAt(i));
        }

        CSVSourceNumeric c = new CSVSourceNumeric(mem, mem + 7, 2);
        Assert.assertEquals(101042, c.getValue());
    }

    @Test
    public void relationalQueryTCPH6() throws IOException, SchemaExtractionException, InterruptedException {
        TCPCSVImporter.importTCPH("/tpch/");
        RuntimeConfiguration.LAZY_PARSING = true;
        CSVScan scan = new CSVScan("table.lineitem");
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
                                                        new FieldConstant<>(new EagerNumeric(24, 0))),
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
                    .option("engine.TraceCompilation", "true")
                    .option("engine.TraceCompilationDetails", "true")
                    .option("engine.CompilationExceptionsArePrinted", "true")
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
