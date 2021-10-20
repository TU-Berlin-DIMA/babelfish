package de.tub.dima.babelfish.ir.lqp.udf.java.typed;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.lqp.udf.UDF;
import de.tub.dima.babelfish.ir.lqp.udf.java.Collector;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

@FunctionalInterface
public interface TypedUDF<IT extends Record, OT extends Record> extends UDF {
    static Schema extractInputSchema(Class<TypedMapFunction> clazz) throws SchemaExtractionException {
        Method callMethod = clazz.getDeclaredMethods()[0];
        Type paramType = callMethod.getGenericParameterTypes()[1];
        Class<Record> param = (Class<Record>) callMethod.getParameterTypes()[1];
        if (paramType instanceof ParameterizedType)
            return RecordUtil.createSchema(param, (ParameterizedType) paramType);
        else
            return RecordUtil.createSchema(param, null);

    }

    static Schema extractOutputSchema(Class<TypedMapFunction> clazz) throws SchemaExtractionException {
        Method callMethod = clazz.getDeclaredMethods()[0];
        Type paramType = callMethod.getGenericParameterTypes()[2];
        Class<Record> param = (Class<Record>) callMethod.getParameterTypes()[2];
        if (paramType instanceof ParameterizedType)
            return RecordUtil.createSchema(param, (ParameterizedType) paramType);
        else
            return RecordUtil.createSchema(param, null);
    }

    // Has to be concrete to prevent error in partial evaluation
    public void call(VirtualFrame frame, IT value1, Collector<OT> collect);
}
