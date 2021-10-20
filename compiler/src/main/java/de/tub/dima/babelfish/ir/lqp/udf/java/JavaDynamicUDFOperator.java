package de.tub.dima.babelfish.ir.lqp.udf.java;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicMapFunction;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicTransform;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;


public class JavaDynamicUDFOperator extends UDFOperator<DynamicTransform> {

    //@CompilerDirectives.CompilationFinal
    // private OutputCollector outputCollector;

    public JavaDynamicUDFOperator(DynamicTransform udf) {
        super(udf);
    }

    public JavaDynamicUDFOperator(DynamicFilterFunction udf) {
        super(udf);
    }

    public JavaDynamicUDFOperator(DynamicMapFunction udf) {
        super(udf);
    }


    @Override
    public void init(OutputCollector outputCollector) {
        //  this.outputCollector = outputCollector;
    }

    @Override
    public void execute(VirtualFrame frame, DynamicRecord input, OutputCollector outputCollector) {
        udf.call(frame, input, outputCollector);
    }

}
