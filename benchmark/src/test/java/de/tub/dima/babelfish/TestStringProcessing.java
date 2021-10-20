package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.GenericSerializer;
import de.tub.dima.babelfish.storage.layout.PhysicalColumnLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.record.*;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;
import de.tub.dima.babelfish.benchmark.string.queries.StringEquals;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.java.typed.TypedFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Predicate;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.relational.Selection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
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

public class TestStringProcessing {


    private Buffer buffer;
    private BufferManager bufferManager;


    @Before
    public void setup() throws IOException, SchemaExtractionException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");
        System.setProperty("python.home", ".");
        bufferManager = new BufferManager();
    }


    @LuthRecord
    public class TextRecord implements Record {
        @MaxLength(length = 7)
        public Text name;
    }

    public boolean split(byte[] array) {
        byte[] a = new byte[]{1,2,3,4};

        boolean result = true;
        for(int i = 0 ; i<a.length;i++){
            result = result & array[i] != a[i];

        }
        return result;

    }

    @Test
    public void executeLoop() throws InterruptedException {
        for(int i = 0; i< 10000;i++){
            split(new byte[]{1,2,3,4});
        }
        Thread.sleep(5000);
        for(int i = 0; i< 1000000;i++){
            split(new byte[]{1,2,3,4});
        }
        Thread.sleep(5000);
    }

    @Test
    public void stringEquals() throws IOException, InterruptedException, SchemaExtractionException {

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        Selection selection1 = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("name", Text.class),
                        new FieldConstant<>(new StringText("philipp")))
        );

        scan.addChild(selection1);
        Sink sink = new Sink.PrintSink();
        selection1.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void stringCopyPython() throws IOException, InterruptedException, SchemaExtractionException {

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        PythonOperator equals = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\treturn rec" +
                "\nlambda rec,ctx: udf(rec,ctx)"));

        scan.addChild(equals);
        Sink sink = new Sink.MemorySink();
        equals.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void stringEqualsOrdersPython() throws IOException, InterruptedException, SchemaExtractionException {
        RuntimeConfiguration.LAZY_STRING_HANDLING = false;
        TCPHImporter.importTCPH("/home/pgrulich/projects/luth-org/tpch-dbgen/");
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        PythonOperator selection = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\tif rec.o_orderpriority.equals(\"5-LOW\"):\n" +
                "\t\tctx(rec)\n" +
                "lambda a,ctx: udf(a,ctx)"));

        projection.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        executeQuery(new LogicalQueryPlan((Sink) StringEquals.stringEqualsPython()));
    }

    @Test
    public void stringEqualsPython() throws IOException, InterruptedException, SchemaExtractionException {
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        PythonOperator equals = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\treturn(rec.name.equals(\"philipp\"))" +
                "\nlambda rec,ctx: udf(rec,ctx)"));

        scan.addChild(equals);
        Sink sink = new Sink.MemorySink();
        equals.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void stringEqualsJS() throws IOException, InterruptedException, SchemaExtractionException {
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(record,ctx)=>{" +
                "return record.name.equals(\"philipi\")" +
                "}"));

        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    static TypedFilterFunction<TextRecord> stringEqualsUDF = new TypedFilterFunction<TextRecord>() {
        @Override
        public boolean filter(TextRecord record) {
            return record.name.length() == 7;
        }
    };

    static DynamicFilterFunction javaDynamicUDF = new DynamicFilterFunction(){

        @Override
        public boolean filter(DynamicRecord input) {
            Text name = input.getValue("name");
            return name.equals(new StringText("philipi", 7));
        }
    };

    @Test
    public void stringEqualsJava() throws IOException, InterruptedException, SchemaExtractionException {
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        UDFOperator selection = new JavaDynamicUDFOperator(javaDynamicUDF);

        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }


    @Test
    public void stringReversePython() throws IOException, InterruptedException, SchemaExtractionException {

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        PythonOperator equals = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\tx = (rec.name.reverse())\n" +
                "\treturn(x)" +
                "\nlambda rec,ctx: udf(rec,ctx)"));

        scan.addChild(equals);
        Sink sink = new Sink.MemorySink();
        equals.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void stringUppercasePython() throws IOException, InterruptedException, SchemaExtractionException {
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        PythonOperator equals = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\tx = (rec.name.uppercase())\n" +
                "\treturn(x)" +
                "\nlambda rec,ctx: udf(rec,ctx)"));

        scan.addChild(equals);
        Sink sink = new Sink.MemorySink();
        equals.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }


    @Test
    public void stringSubstringPython() throws IOException, InterruptedException, SchemaExtractionException {

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        PythonOperator equals = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\tx = (rec.name.substring(0,2))\n" +
                "\treturn(x)" +
                "\nlambda rec,ctx: udf(rec,ctx)"));

        scan.addChild(equals);
        Sink sink = new Sink.MemorySink();
        equals.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }




    @Test
    public void stringConcatPython() throws IOException, InterruptedException, SchemaExtractionException {

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        PythonOperator equals = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\tx = (rec.name.reverse().concat(\"test\").concat(\"test\"))\n" +
                "\treturn(x)" +
                "\nlambda rec,ctx: udf(rec,ctx)"));

        scan.addChild(equals);
        Sink sink = new Sink.MemorySink();
        equals.addChild(sink);
        executeQuery(new LogicalQueryPlan(sink));
    }

    @Test
    public void stringSplitPython() throws IOException, InterruptedException, SchemaExtractionException {

        Schema schema = RecordUtil.createSchema(TextRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long records = 1_000_000;
        PhysicalLayout physicalLayout = new PhysicalColumnLayout(physicalSchema, physicalSchema.getFixedRecordSize() * records);
        buffer = bufferManager.allocateBuffer(new Unit.Bytes(physicalSchema.getFixedRecordSize() * records + 1024));
        physicalLayout.initBuffer(buffer);


        for (int i = 0; i < records; i++) {
            TextRecord record = new TextRecord();
            record.name = new StringText("philipi");
            GenericSerializer.serialize(schema, physicalLayout, buffer, record);
        }

        Catalog.getInstance().registerBuffer("table.lineitem", buffer);
        Catalog.getInstance().registerLayout("table.lineitem", physicalLayout);

        Scan scan = new Scan("table.lineitem");

        PythonOperator equals = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\tx = (rec.name.split(\"l\")[0])\n" +
                "\treturn(x)" +
                "\nlambda rec,ctx: udf(rec,ctx)"));

        scan.addChild(equals);
        Sink sink = new Sink.MemorySink();
        equals.addChild(sink);
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
            Context context = Context.newBuilder("luth", "js",  "python")
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
