package de.tub.dima.babelfish.benchmark.analytics.queries;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.benchmark.string.queries.StringEquals;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicTransform;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.storage.text.leaf.PointerRope;
import de.tub.dima.babelfish.storage.text.leaf.Split;
import de.tub.dima.babelfish.storage.text.operations.SplittedRope;
import de.tub.dima.babelfish.storage.text.operations.SubstringRope;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class WordCount {

    static DynamicTransform wordCountUDF = new DynamicTransform() {
        @Override
        public void call(VirtualFrame ctx, DynamicRecord input, UDFOperator.OutputCollector output) {
            PointerRope text = input.getValue("o_comment");
            SplittedRope splits = Split.split(text, ' ');
            for (int i = 0; i < splits.length(); i++) {
                DynamicRecord outputRecort = output.createOutputRecord();
                SubstringRope sub = (SubstringRope) splits.get(i);
                outputRecort.setValue("word", sub);
                output.emitRecord(ctx, outputRecort);
            }
        }
    };

    /**
     * def word_count(rec, ctx):
     * words = rec.o_comment.split(" ")
     * for word in words:
     * word = word.lower()
     * ctx.emit((word,1))
     *
     * @return
     */
    public static LogicalOperator wordCountPython() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(
                new FieldReference("o_comment", Text.class, 79)
        );
        scan.addChild(projection);
        PythonOperator selection = new PythonOperator(new PythonUDF("def wordcount(rec,ctx):\n" +
                "\twords = rec.o_comment.split(\" \")\n" +
                //"\tlength = len(words)\n" +
                "\tfor w in words:\n" +
                "\t\tctx((w))\n" +
                //"\tctx(rec)\n" +
                "lambda rec,ctx:wordcount(rec,ctx)"));
        projection.addChild(selection);
        //GroupBy groupBy = new GroupBy(new KeyGroup(new FieldReference<>("value_1", Text.class, 15)), new Aggregation.Sum(new FieldReference<>("value_2", Int_32.class)));
        //selection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static LogicalOperator wordCountJavaScript() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(
                new FieldReference("o_comment", Text.class, 79)
        );
        scan.addChild(projection);
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(rec,ctx)=>{" +
                "const words = rec.o_comment.split(\" \");" +
                "for (let i = 0; i< words.length;i++){" +
                //"for (word of words){" +
                "ctx(words[i]);}" +
                //"ctx(words[1]);" +
                //"}" +
                "}"));
        projection.addChild(selection);
        //GroupBy groupBy = new GroupBy(new KeyGroup(new FieldReference<>("value_1", Text.class, 15)), new Aggregation.Sum(new FieldReference<>("value_2", Int_32.class)));
        //selection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static LogicalOperator wordCountJava() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(
                new FieldReference("o_comment", Text.class, 79)
        );
        scan.addChild(projection);

        UDFOperator selection = new JavaDynamicUDFOperator(wordCountUDF);
        projection.addChild(selection);

        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }


    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "python":
                return wordCountPython();
            case "java":
                return wordCountJava();
            case "js":
                return wordCountJavaScript();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }


}
