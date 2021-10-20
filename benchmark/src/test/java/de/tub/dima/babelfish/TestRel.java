package de.tub.dima.babelfish;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
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

import static de.tub.dima.babelfish.benchmark.parser.TcpHDataReader.readFile;

public class TestRel {

    private Buffer buffer;
    private BufferManager outputBufferManager;

    @Before
    public void setup() throws IOException, SchemaExtractionException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");

        buffer = readFile("/home/pgrulich/projects/luth-org/tpch-dbgen/lineitem.tbl");
        outputBufferManager = new BufferManager();
        Catalog.getInstance().getLayout("lineitem");

    }

    @Test
    public void executeScanSinkQuery() throws IOException, InterruptedException, SchemaExtractionException {
        Scan scan = new Scan("table.lineitem");
        Sink sink = new Sink.MemorySink();
        scan.addChild(sink);
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Scan) scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }


    static Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth", "js")
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
