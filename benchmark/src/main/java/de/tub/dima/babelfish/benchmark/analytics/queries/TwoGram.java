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
import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.variableLengthType.SplittedText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class TwoGram {

    static DynamicTransform twoGramUDF = new DynamicTransform() {
        @Override
        public void call(VirtualFrame ctx, DynamicRecord input, UDFOperator.OutputCollector output) {
            AbstractRope text = input.getValue("o_comment");
            SplittedText splits = text.split(' ');
            int length = splits.length();
            int index = 1;
            while (index < length) {
                Text twoWord = splits.get(index - 1).concat(splits.get(index));
                DynamicRecord outputRecort = output.createOutputRecord();
                outputRecort.setValue("word", twoWord);
                output.emitRecord(ctx, outputRecort);
                index++;
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
    public static LogicalOperator twoGramPython() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(
                new FieldReference("o_comment", Text.class, 79)
        );
        scan.addChild(projection);
        PythonOperator selection = new PythonOperator(new PythonUDF("def wordcount(rec,ctx):\n" +
                "\twords = rec.o_comment.split(\" \")\n" +
                "\tlength = len(words)\n" +
                "\tindex = 1\n" +
                "\twhile index < length:\n" +
                "\t\ttwogram = words[index-1].concat(words[index])\n" +
                "\t\tindex = index +1\n" +
                "\t\tctx(twogram)\n" +
                //"\tctx(rec)\n" +
                "lambda rec,ctx:wordcount(rec,ctx)"));
        projection.addChild(selection);
        //GroupBy groupBy = new GroupBy(new KeyGroup(new FieldReference<>("value_1", Text.class, 15)), new Aggregation.Sum(new FieldReference<>("value_2", Int_32.class)));
        //selection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static LogicalOperator twoGramJavaScript() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(
                new FieldReference("o_comment", Text.class, 79)
        );
        scan.addChild(projection);
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(rec,ctx)=>{" +
                "const words = rec.o_comment.split(\" \");" +
                "const length = words.length;" +
                "let index = 1;" +
                "while (index < length){" +
                "const twogram = words[index-1].concat(words[index]);" +
                "index++;" +
                "ctx(twogram);}" +
                "}"));
        projection.addChild(selection);
        //GroupBy groupBy = new GroupBy(new KeyGroup(new FieldReference<>("value_1", Text.class, 15)), new Aggregation.Sum(new FieldReference<>("value_2", Int_32.class)));
        //selection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static LogicalOperator twoGramJava() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(
                new FieldReference("o_comment", Text.class, 79)
        );
        scan.addChild(projection);

        UDFOperator selection = new JavaDynamicUDFOperator(twoGramUDF);
        projection.addChild(selection);

        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }


    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "python":
                return twoGramPython();
            case "java":
                return twoGramJava();
            case "js":
                return twoGramJavaScript();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }


}
