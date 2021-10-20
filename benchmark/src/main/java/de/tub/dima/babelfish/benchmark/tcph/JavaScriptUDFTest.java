package de.tub.dima.babelfish.benchmark.tcph;

import de.tub.dima.babelfish.benchmark.parser.TcpHDataReader;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;

import java.io.IOException;

public class JavaScriptUDFTest {
    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");

        Buffer buffer = TcpHDataReader.readFile("/home/pgrulich/projects/luth-org/tpch-dbgen/lineitem.tbl");
        BufferManager outputBufferManager = new BufferManager();


        Scan scan = new Scan("table.lineitem");
        // UDFOperator selection = new JavaTypedUDFOperator<>(this.filterTypedUDF);
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(x)=>{x.getValue('l_quantity')<=24;}"));
        scan.addChild(selection);
        Sink.PrintSink printSink = new Sink.PrintSink();
        selection.addChild(printSink);


    }
}
