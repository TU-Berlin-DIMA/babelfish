package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.GenericSerializer;
import de.tub.dima.babelfish.storage.layout.PhysicalColumnLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptScalarUDF;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptTransformUDF;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
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

import static de.tub.dima.babelfish.benchmark.tcph.queries.Query6.javaScriptTCPH6Query;

public class TestJavaScriptUDF {

    private Buffer buffer;
    private BufferManager outputBufferManager;

    @Before
    public void setup() throws IOException, SchemaExtractionException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");

        //buffer = readFile("/home/pgrulich/projects/luth-org/tpch-dbgen/lineitem.tbl");
        outputBufferManager = new BufferManager();
        Catalog.getInstance().getLayout("lineitem");

    }


    public static Sink javaScriptReturnUDF_Exception() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptScalarUDF("(x)=>{return 10;}"));
        scan.addChild(selection);
        Sink sink = new Sink.PrintSink();
        selection.addChild(sink);
        return sink;
    }

    public static Sink javaScriptReturnSameUDF() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptScalarUDF("(x)=>{return x;}"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static Sink javaScriptReturnUDF() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptScalarUDF("(x)=>{return {'test':x.l_discount};}"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static Sink javaScriptCallbackUDF() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptTransformUDF("(x,ctx)=>{" +
                "ctx.emit({'test': x.l_discount });" +
                "}"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static Sink javaScriptWithState() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptScalarUDF("(x,ctx)=>{" +
                "const st = ctx.stateManager;" +
                "let y = st.getState('test', 0);" +
                "y.val = y.val++;" +
                "let z  = st.getState('test');" +
                "return {'test': z.val + y.val}" +
                "}"));
        scan.addChild(selection);
        Sink sink = new Sink.PrintSink();
        selection.addChild(sink);
        return sink;
    }


    @Test
    public void executeJS_tcphFilterUDF() throws IOException, InterruptedException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");
        LogicalOperator scan = javaScriptTCPH6Query();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Sink) scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() < 100)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @Test
    public void executeJS_ExceptionScalarUDF() throws IOException, InterruptedException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");
        LogicalOperator scan = javaScriptReturnUDF_Exception();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Sink) scan));
        Thread.sleep(1000);
        try {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
        } catch (RuntimeException e) {
            return;
        }
        Assert.fail();
    }

    @Test
    public void executeJSCallbackUDF() throws IOException, InterruptedException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");

        Sink sink = javaScriptCallbackUDF();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() < 20)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }


    @Test
    public void executeJSWithState() throws IOException, InterruptedException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");

        LogicalOperator scan = javaScriptWithState();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Scan) scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() < 50)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @Test
    public void executeJSReturnSameUDF() throws IOException, InterruptedException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");

        Sink scan = javaScriptReturnSameUDF();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @Test
    public void executeJSReturnUDF() throws IOException, InterruptedException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");

        Sink scan = javaScriptReturnUDF();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @LuthRecord
    public class TestRecord implements Record {
        public int name;
    }

    @Test
    public void testEscape() throws IOException, InterruptedException, SchemaExtractionException {

        Schema schema = RecordUtil.createSchema(TestRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = outputBufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TestRecord record = new TestRecord();
            record.name = i % 5;
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(x,ctx)=>{" +
                "x.test = x.name * 2;" +
                "return x;" +
                "}"));

        //scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        scan.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    public void executeQuery(LogicalQueryPlan plan) throws IOException, InterruptedException, SchemaExtractionException {


        Thread.sleep(1000);
        Value executableQuery = submitQuery(plan);
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
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
            Context context = Context.newBuilder("luth", "js")
                    // .option("engine.TraceMethodExpansion", "true")
                    //.option("engine.TraceCompilationAST", "true")
                    .option("engine.TraceCompilation", "true")
                    .option("engine.TraceCompilationDetails", "true")
                    .option("engine.CompilationExceptionsArePrinted", "true")
                    //  .option("engine.LanguageAgnosticInlining", "true")
                    .option("engine.BackgroundCompilation", "false")
                    //.option("engine.IterativePartialEscape", "true")
                    //.option("engine.IterativePartialEscape", "true")
                    //.option("engine.Inlining", "true")
                    // .option("engine.CompileImmediately", "true")
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
