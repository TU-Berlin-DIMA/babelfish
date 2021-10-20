package de.tub.dima.babelfish;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.storage.layout.fields.*;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.tuple.*;
import de.tub.dima.babelfish.typesytem.udt.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.*;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.Collector;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaTypedUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicTransform;
import de.tub.dima.babelfish.ir.lqp.udf.java.typed.TypedUDF;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import org.graalvm.polyglot.*;
import org.graalvm.polyglot.io.*;
import org.junit.*;

import java.io.*;

public class UDFExecutionTest implements Serializable {

    @Test
    public void executeTest() throws IOException, InterruptedException {
        /*RuntimeFactory instace = RuntimeFactory.getInstance();

        RuntimeFactory.getRuntime();
        *
         */
        PhysicalSchema schema = new PhysicalSchema.Builder()
                .addField(new Int_32_PhysicalField("id"))
                .addField(new Int_32_PhysicalField("age")).build();
        PhysicalLayout physicalLayout = new PhysicalRowLayout(schema);

        Catalog.getInstance().registerLayout("User", physicalLayout);

        //instace.addPlugin(new MemoryAccessGraphPlugin());
        Scan scan = new Scan("User");

        DynamicFilterFunction filterFunction = new DynamicFilterFunction() {
            @Override
            public boolean filter(DynamicRecord input) {
                Date date = new Date("1991-05-05");
                Int_32 age = input.getValue("age");
                return  age.asInt() < date.getUnixTs();
            }
        };

       /* DynamicTransform udf = new DynamicTransform() {
            @Override
            public void call(VirtualFrame ctx, DynamicRecord input, UDFOperator.OutputCollector output) {
                Date date = new Date("1991-05-05");
                Int_32 age = input.getValue("age");
                if (age.asInt() < date.getUnixTs()) {
                    output.emitRecord(ctx, input);
                }
            }
        };*/
        UDFOperator udfOperator = new JavaDynamicUDFOperator(filterFunction);
        scan.addChild(udfOperator);
        Sink sink = new Sink();
        udfOperator.addChild(sink);

//        LuthLogicalOpratorTreeDump.dumpPlan(sink);

        Context context = Context.newBuilder("luth", "js").allowAllAccess(true).build();

        Thread.sleep(1000);

        BufferManager bufferManager = new BufferManager();
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(500000));

        physicalLayout.initBuffer(buffer);
        for (int i = 0; i < 10; i++) {
            GenericSerializer.addRecord(buffer);
            GenericSerializer.setField(physicalLayout, buffer, i, 0, new Eager_Int_32(1));
            GenericSerializer.setField(physicalLayout, buffer, i, 1, new Eager_Int_32(i));
        }


        BufferArgument luthBufferArgument = new BufferArgument(buffer, bufferManager);

        Value pipeline =
                submitQuery(new LogicalQueryPlan(scan));
        for (int i = 0; i < 100; i++)

            pipeline.execute(luthBufferArgument
            );

        System.out.println("waiting");

        Thread.sleep(50000);
        for (int i = 0; i < 100; i++)
            pipeline.execute(luthBufferArgument);
        Thread.sleep(5000000);

    }

    static Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth").allowAllAccess(true).build();
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


    @Test
    public void executeTest2() throws IOException, InterruptedException {
        //RuntimeFactory instace = RuntimeFactory.getInstance();

        PhysicalSchema schema = new PhysicalSchema.Builder()
                .addField(new Int_32_PhysicalField("f1"))
                .addField(new Int_32_PhysicalField("f2")).build();
        PhysicalLayout physicalLayout = new PhysicalRowLayout(schema);

        Catalog.getInstance().registerLayout("User", physicalLayout);

        //instace.addPlugin(new BFRecordGraphPlugins());
        //instace.addPlugin(new JavaObjectGraphPlugins());
        Scan scan = new Scan("User");
        TypedUDF<Tuple2<Int_32, Int_32>, Tuple2<Int_32, Int_32>> udf = new TypedUDF<Tuple2<Int_32, Int_32>, Tuple2<Int_32, Int_32>>() {
            @Override
            public void call(VirtualFrame frame, Tuple2<Int_32, Int_32> input, Collector<Tuple2<Int_32, Int_32>> output) {
                if (input.f2.asInt() > 21)
                    output.emit(frame, input);
            }
        };
        UDFOperator udfOperator = new JavaTypedUDFOperator(udf);
        scan.addChild(udfOperator);
        Sink sink = new Sink();
        udfOperator.addChild(sink);

//        LuthLogicalOpratorTreeDump.dumpPlan(sink);

        Context context = Context.newBuilder("luth", "js").allowAllAccess(true).build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new LogicalQueryPlan(scan));
        oos.flush();
        oos.close();

        Thread.sleep(1000);

        BufferManager bufferManager = new BufferManager();
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(5000));

        physicalLayout.initBuffer(buffer);
        for (int i = 0; i < 42; i++) {
            GenericSerializer.addRecord(buffer);
            GenericSerializer.setField(physicalLayout, buffer, i, 0, new Eager_Int_32(1));
            GenericSerializer.setField(physicalLayout, buffer, i, 1, new Eager_Int_32(i));
        }


        BufferArgument luthBufferArgument = new BufferArgument(buffer, bufferManager);

        Source s = Source.newBuilder("luth", ByteSequence.create(baos.toByteArray()), "testPlan").build();
        Value pipeline = context.eval(s);
        for (int i = 0; i < 100; i++)

            pipeline.execute(luthBufferArgument
            );

        System.out.println("waiting");

        Thread.sleep(10000);

        System.out.println("running 2 ");

        for (int i = 0; i < 42; i++) {
            GenericSerializer.addRecord(buffer);
            GenericSerializer.setField(physicalLayout, buffer, i, 0, new Eager_Int_32(1));
            GenericSerializer.setField(physicalLayout, buffer, i, 1, new Eager_Int_32(1000));
        }

        for (int i = 0; i < 100; i++)
            pipeline.execute(luthBufferArgument);
        Thread.sleep(5000000);

    }

    @LuthRecord
    public class TestRecord implements Record{
        public int f1;
        public int f2;
    }

    @Test
    public void executeTest3() throws IOException, InterruptedException {
       // RuntimeFactory instace = RuntimeFactory.getInstance();

        PhysicalSchema schema = new PhysicalSchema.Builder()
                .addField(new Int_32_PhysicalField("f1"))
                .addField(new Int_32_PhysicalField("f2")).build();
        PhysicalLayout physicalLayout = new PhysicalRowLayout(schema);

        Catalog.getInstance().registerLayout("User", physicalLayout);

      //  instace.addPlugin(new BFRecordGraphPlugins());
       // instace.addPlugin(new JavaObjectGraphPlugins());
        Scan scan = new Scan("User");
        TypedUDF<TestRecord, TestRecord> udf = new TypedUDF<TestRecord, TestRecord>() {
            @Override
            public void call(VirtualFrame frame, TestRecord input, Collector<TestRecord> output) {
                if (input.f2 > 21)
                    output.emit(frame, input);
            }
        };
        UDFOperator udfOperator = new JavaTypedUDFOperator(udf);
        scan.addChild(udfOperator);
        Sink sink = new Sink();
        udfOperator.addChild(sink);

//        LuthLogicalOpratorTreeDump.dumpPlan(sink);

        Context context = Context.newBuilder("luth", "js").allowAllAccess(true).build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new LogicalQueryPlan(scan));
        oos.flush();
        oos.close();

        Thread.sleep(1000);

        BufferManager bufferManager = new BufferManager();
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(5000));

        physicalLayout.initBuffer(buffer);
        for (int i = 0; i < 42; i++) {
            GenericSerializer.addRecord(buffer);
            GenericSerializer.setField(physicalLayout, buffer, i, 0, new Eager_Int_32(1));
            GenericSerializer.setField(physicalLayout, buffer, i, 1, new Eager_Int_32(i));
        }


        BufferArgument luthBufferArgument = new BufferArgument(buffer, bufferManager);

        Source s = Source.newBuilder("luth", ByteSequence.create(baos.toByteArray()), "testPlan").build();
        Value pipeline = context.eval(s);
        for (int i = 0; i < 100; i++)

            pipeline.execute(luthBufferArgument
            );

        System.out.println("waiting");

        Thread.sleep(10000);

        System.out.println("running 2 ");

        for (int i = 0; i < 42; i++) {
            GenericSerializer.addRecord(buffer);
            GenericSerializer.setField(physicalLayout, buffer, i, 0, new Eager_Int_32(1));
            GenericSerializer.setField(physicalLayout, buffer, i, 1, new Eager_Int_32(1000));
        }

        for (int i = 0; i < 100; i++)
            pipeline.execute(luthBufferArgument);
        Thread.sleep(5000000);

    }


    @Test
    public void tcphTest() throws IOException, InterruptedException {
        //RuntimeFactory instace = RuntimeFactory.getInstance();
        //instace.addPlugin(new BFRecordGraphPlugins());
        //instace.addPlugin(new JavaObjectGraphPlugins());

        PhysicalSchema dateSchema = new PhysicalSchema.Builder().addField(new Int_64_PhysicalField("unixTs")).build();


        PhysicalSchema schema = new PhysicalSchema.Builder()
                .addField(new PhysicalUDTField("l_shipdate", Date.class, dateSchema))
                .addField(new Fload_32_PhysicalField("l_discount"))
                .addField(new Fload_32_PhysicalField("l_quantity"))
                .addField(new Fload_32_PhysicalField("l_extendedprice"))
                .addField(new Int_32_PhysicalField("l_quantity"))
                .build();
        PhysicalLayout physicalLayout = new PhysicalRowLayout(schema);

        Catalog.getInstance().registerLayout("lineitem", physicalLayout);

        Scan scan = new Scan("lineitem");
        DynamicTransform udf = (ctx, input, output) -> {
            Date l_shipdate = input.getValue("l_shipdate");
            if (l_shipdate.getUnixTs() >= new Date("1994-01-01").getUnixTs()) {
                output.emitRecord(ctx, input);
            }
        };
        UDFOperator udfOperator = new JavaDynamicUDFOperator(udf);
        scan.addChild(udfOperator);
        Sink sink = new Sink();
        udfOperator.addChild(sink);

//        LuthLogicalOpratorTreeDump.dumpPlan(sink);

        Context context = Context.newBuilder("luth", "js").allowAllAccess(true).build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(new LogicalQueryPlan(scan));
        oos.flush();
        oos.close();

        Thread.sleep(1000);

        BufferManager bufferManager = new BufferManager();
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(500000));

        physicalLayout.initBuffer(buffer);
        for (int i = 0; i < 100; i++) {
            GenericSerializer.addRecord(buffer);
            GenericSerializer.setField(physicalLayout, buffer, i, 0, new Date("1994-01-01"));
            GenericSerializer.setField(physicalLayout, buffer, i, 1, new Eager_Float_32(i));
            GenericSerializer.setField(physicalLayout, buffer, i, 2, new Eager_Float_32(i));
            GenericSerializer.setField(physicalLayout, buffer, i, 3, new Eager_Float_32(i));
            GenericSerializer.setField(physicalLayout, buffer, i, 4, new Eager_Int_32(i));
        }


        BufferArgument luthBufferArgument = new BufferArgument(buffer, bufferManager);

        Source s = Source.newBuilder("luth", ByteSequence.create(baos.toByteArray()), "testPlan").build();
        Value pipeline = context.eval(s);
        for (int i = 0; i < 100; i++)

            pipeline.execute(luthBufferArgument
            );

        System.out.println("waiting");

        Thread.sleep(50000);
        for (int i = 0; i < 100; i++)
            pipeline.execute(luthBufferArgument);
        Thread.sleep(5000000);

    }

}
