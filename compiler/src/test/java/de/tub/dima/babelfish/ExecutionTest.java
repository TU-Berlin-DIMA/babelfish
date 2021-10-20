package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.storage.layout.fields.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import org.graalvm.polyglot.*;
import org.graalvm.polyglot.io.*;
import org.junit.*;

import java.io.*;

public class ExecutionTest {

    @Test
    public void executeTest() throws IOException, InterruptedException {
        //RuntimeFactory instace = RuntimeFactory.getInstance();

        PhysicalSchema schema = new PhysicalSchema.Builder()
                .addField(new Int_32_PhysicalField("id"))
                .addField(new Int_32_PhysicalField("age")).build();
        PhysicalLayout physicalLayout = new PhysicalRowLayout(schema);

        Catalog.getInstance().registerLayout("User", physicalLayout);


        //extLoader.add(factory.getClass().getName());

       // instace.addPlugin(new BFRecordGraphPlugins());
        Scan scan = new Scan("User");
        scan.addChild(new Sink());

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

        Thread.sleep(50000);
        for (int i = 0; i < 100; i++)
            pipeline.execute(luthBufferArgument);
        Thread.sleep(5000000);

    }

}
