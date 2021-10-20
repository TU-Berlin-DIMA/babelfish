package de.tub.dima.babelfish.ir.lqp.udf.java.typed;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.lqp.udf.java.Collector;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

@FunctionalInterface
public interface TypedMapFunction<I extends Record, O extends Record> extends TypedUDF<I, O> {

    static Schema extractInputSchema(Class<TypedMapFunction> clazz) throws SchemaExtractionException {
        Method callMethod = clazz.getDeclaredMethods()[0];
        Type paramType = callMethod.getGenericParameterTypes()[0];
        Class<Record> param = (Class<Record>) callMethod.getParameterTypes()[0];
        return RecordUtil.createSchema(param, paramType);
    }

    static Schema extractOutputSchema(Class<TypedMapFunction> clazz) throws SchemaExtractionException {
        Method callMethod = clazz.getDeclaredMethods()[0];
        Type paramType = callMethod.getGenericReturnType();
        Class<Record> param = (Class<Record>) callMethod.getReturnType();
        return RecordUtil.createSchema(param, paramType);
    }

    O map(I inputRecord);

    default void call(VirtualFrame frame, I typedInputRecord, Collector<O> collect) {
        collect.emit(frame, map(typedInputRecord));
    }
}
