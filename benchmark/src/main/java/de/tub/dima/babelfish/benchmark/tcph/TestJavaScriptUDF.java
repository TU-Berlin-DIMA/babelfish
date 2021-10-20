package de.tub.dima.babelfish.benchmark.tcph;

import de.tub.dima.babelfish.BufferArgument;
import de.tub.dima.babelfish.benchmark.parser.TcpHDataReader;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class TestJavaScriptUDF {


    public static LogicalOperator javaScriptReturnUDF_Exception() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(x)=>{return 10;}"));
        scan.addChild(selection);
        Sink sink = new Sink.PrintSink();
        selection.addChild(sink);
        return scan;
    }

    public static LogicalOperator javaScriptReturnUDF() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(x)=>{return {'test':x.l_discount};}"));
        scan.addChild(selection);
        Sink sink = new Sink.PrintSink();
        selection.addChild(sink);
        return scan;
    }

    public static LogicalOperator javaScriptCallbackUDF() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(x,ctx)=>{ctx(x);}"));
        scan.addChild(selection);
        Sink sink = new Sink.PrintSink();
        selection.addChild(sink);
        return scan;
    }

    static Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth", "js")
                    // .option("inspect","true")
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

    public void executeTest() throws IOException, InterruptedException, SchemaExtractionException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");

        Buffer buffer = TcpHDataReader.readFile("/home/pgrulich/projects/luth-org/tpch-dbgen/lineitem.tbl");
        BufferManager outputBufferManager = new BufferManager();
        Catalog.getInstance().getLayout("lineitem");

        LogicalOperator scan = javaScriptReturnUDF();
        System.out.println("Submit");
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Scan) scan));
        //System.out.println("Start Execution");
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Thread.sleep(100000);
    }

}
