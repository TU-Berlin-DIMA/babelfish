package de.tub.dima.babelfish.ir.lqp.udf.java.dynamic;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;

@FunctionalInterface
public interface DynamicMapFunction extends DynamicTransform {
    public void map(DynamicRecord input, DynamicRecord output);

    default void call(VirtualFrame ctx, DynamicRecord input, UDFOperator.OutputCollector output) {
        DynamicRecord outputRecord = output.createOutputRecord();
        map(input, outputRecord);
        output.emitRecord(ctx, outputRecord);
    }
}
