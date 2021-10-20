package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.tuple.Tuple1;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.benchmark.datatypes.Lineitem;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaTypedUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicMapFunction;
import de.tub.dima.babelfish.ir.lqp.udf.java.typed.TypedMapFunction;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
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

import static de.tub.dima.babelfish.benchmark.tcph.queries.Query6.javaTcph6TypedUDFQuery;

public class TestJavaUDF {

    private Buffer buffer;
    private BufferManager outputBufferManager;

    @Before
    public void setup() throws IOException, SchemaExtractionException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");

        TCPHImporter.importTCPH("/tpch/");
        outputBufferManager = new BufferManager();
        Catalog.getInstance().getLayout("lineitem");

    }


    private static DynamicMapFunction javaDynamicUDF = new DynamicMapFunction(){

        @Override
        public void map(DynamicRecord input, DynamicRecord output) {
            EagerNumeric l_discount = input.getValue("l_discount");
            output.setValue("test", new EagerNumeric(l_discount.getValue()*2,2));
        }
    };

    private static TypedMapFunction<Lineitem, Tuple1<Float_32>> javaTypedUDF = new TypedMapFunction<Lineitem, Tuple1<Float_32>>(){
        @Override
        public Tuple1<Float_32> map(Lineitem record) {
            float revenue = record.l_extendedprice.getValue() * record.l_discount.getValue();
            return new Tuple1<>(new Eager_Float_32(revenue));
        }
    };


    public static LogicalOperator javaReturnUDFQuery() {
        Scan scan = new Scan("table.lineitem");

        UDFOperator mapOperator = new JavaTypedUDFOperator<>(javaTypedUDF);
        scan.addChild(mapOperator);
        Sink sink = new Sink.PrintSink();
        mapOperator.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaReturnDynamicUDFQuery() {
        Scan scan = new Scan("table.lineitem");

        UDFOperator mapOperator = new JavaDynamicUDFOperator(javaDynamicUDF);
        scan.addChild(mapOperator);
        Sink sink = new Sink.MemorySink();
        mapOperator.addChild(sink);
        return sink;
    }



    @Test
    public void executeJavaUDF() throws IOException, InterruptedException, SchemaExtractionException {

        LogicalOperator scan = javaReturnUDFQuery();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Sink) scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            if(time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }


    @Test
    public void executeJavaDynamicUDF() throws IOException, InterruptedException, SchemaExtractionException {

        LogicalOperator scan = javaReturnDynamicUDFQuery();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Sink) scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            if(time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @Test
    public void executeJavaTypedTCPH6UDF() throws IOException, InterruptedException, SchemaExtractionException {

        LogicalOperator scan = javaTcph6TypedUDFQuery();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Sink) scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            if(time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }


    static Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth", "js")
                    .option("engine.CompilationExceptionsArePrinted", "true")
                  // .option("engine.IterativePartialEscape", "true")
                   // .option("engine.Inlining", "false")
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
