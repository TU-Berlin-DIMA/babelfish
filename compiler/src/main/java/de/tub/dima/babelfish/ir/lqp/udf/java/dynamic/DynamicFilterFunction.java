package de.tub.dima.babelfish.ir.lqp.udf.java.dynamic;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.profiles.ConditionProfile;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;


@FunctionalInterface
public interface DynamicFilterFunction extends DynamicTransform {
    boolean filter(DynamicRecord input);
    final ConditionProfile condition = ConditionProfile.createCountingProfile();


    default void call(VirtualFrame ctx, DynamicRecord input, UDFOperator.OutputCollector output) {
        if (condition.profile(filter(input)))
            output.emitRecord(ctx, input);
    }
}
