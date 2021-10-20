package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.storage.layout.fields.*;
import de.tub.dima.babelfish.typesytem.udt.*;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import org.graalvm.polyglot.*;
import org.graalvm.polyglot.io.*;
import org.junit.*;

import java.io.*;

public class UDTest implements Serializable {

    @Test
    public void executeTest() throws IOException, InterruptedException {
        System.setProperty("luth.home",".");
        System.setProperty("js.home",".");
        PhysicalSchema udtSchema = new PhysicalSchema
                .Builder()
                .addField(new Int_32_PhysicalField("x"))
                .addField(new Int_32_PhysicalField("y")).build();

        PhysicalSchema schema = new PhysicalSchema.Builder()
                .addField(new PhysicalUDTField<>("t",Point.class, udtSchema))
                .build();
        PhysicalLayout physicalLayout = new PhysicalRowLayout(schema);

        System.out.println(physicalLayout);

        Catalog.getInstance().registerLayout("User", physicalLayout);


        Scan scan = new Scan("User");
        Sink sink = new Sink();
        scan.addChild(sink);

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
        for (int i = 0; i < 42000; i++) {
            GenericSerializer.addRecord(buffer);
            GenericSerializer.setField(physicalLayout, buffer, i, 0, new Point(i, i));
        }


        BufferArgument luthBufferArgument = new BufferArgument(buffer, bufferManager);

        Source s = Source.newBuilder("luth", ByteSequence.create(baos.toByteArray()), "testPlan").build();
        Value pipeline = context.eval(s);
        for (int i = 0; i < 100; i++)
            pipeline.execute(luthBufferArgument);

        System.out.println("waiting");

        Thread.sleep(50000);
        for (int i = 0; i < 100; i++)
            pipeline.execute(luthBufferArgument);
        Thread.sleep(5000000);

    }

}
