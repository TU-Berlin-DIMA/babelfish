package de.tub.dima.babelfish.ir.lqp.udf.java.dynamic;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.lqp.udf.UDF;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;

public interface DynamicTransform extends UDF {

    void call(VirtualFrame ctx, DynamicRecord input, UDFOperator.OutputCollector output);

}
