package de.tub.dima.babelfish.ir.lqp.udf.java;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.lqp.Operator;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.typed.TypedFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.java.typed.TypedMapFunction;
import de.tub.dima.babelfish.ir.lqp.udf.java.typed.TypedUDF;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.typesytem.schema.field.SchemaField;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Operator(name = "TypedUDF")
public class JavaTypedUDFOperator<T extends Record> extends UDFOperator<TypedUDF> {

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    public final SchemaField[] inputSchemaFields;
    @CompilerDirectives.CompilationFinal(dimensions = 1)
    public final SchemaField[] outputSchemaFields;
    private final Schema inputSchema;
    private final Class<? extends Record> inputClazz;
    private final Schema outputSchema;
    private final Class<? extends Record> outputClazz;
    @CompilerDirectives.CompilationFinal(dimensions = 1)
    public transient Field[] typedInputRecordFields;
    @CompilerDirectives.CompilationFinal(dimensions = 1)
    public transient Field[] typedOutputRecordFields;

    public JavaTypedUDFOperator(TypedUDF udf) {
        super(udf);
        inputSchema = extractInputSchema(udf);
        outputSchema = extractOutputSchema(udf);
        inputClazz = inputSchema.getRecordClass();
        outputClazz = outputSchema.getRecordClass();
        inputSchemaFields = inputSchema.getFields();
        outputSchemaFields = outputSchema.getFields();
        typedInputRecordFields = inputClazz.getFields();
        typedOutputRecordFields = outputClazz.getFields();
    }


    public JavaTypedUDFOperator(TypedMapFunction<T, ?> udf) {
        this((TypedUDF) udf);
    }

    public JavaTypedUDFOperator(TypedFilterFunction<T> udf) {
        this((TypedUDF) udf);
    }

    public Record createTypedRecord() {
        try {
            return UnsafeUtils.allocateInstance(inputClazz);
        } catch (InstantiationException e) {

        }
        return null;
    }


    @Override
    public void init(OutputCollector outputCollector) {
        if (typedInputRecordFields == null && CompilerDirectives.inInterpreter()) {
            typedInputRecordFields = inputClazz.getFields();
            typedOutputRecordFields = outputClazz.getFields();
        }
    }

    @Override
    public void execute(VirtualFrame frame, DynamicRecord dynamicInputObject, OutputCollector outputCollector) {

    }


    public Schema extractInputSchema(TypedUDF udf) {
        try {
            Method method = udf.getClass().getInterfaces()[0].getMethod("extractInputSchema", Class.class);
            Schema schema = (Schema) method.invoke(null, udf.getClass());
            return schema;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Schema extractOutputSchema(TypedUDF udf) {
        try {
            Method method = udf.getClass().getInterfaces()[0].getMethod("extractOutputSchema", Class.class);
            Schema schema = (Schema) method.invoke(null, udf.getClass());
            return schema;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }
/*
        Method callMethod = udf.getClass().getDeclaredMethods()[0];
        Type paramType = callMethod.getGenericParameterTypes()[0];
        Class<Record> param = (Class<Record>) callMethod.getParameterTypes()[0];
        if (callMethod.getGenericParameterTypes().length > 1) {
            paramType = callMethod.getGenericParameterTypes()[1];
            param = (Class<Record>) callMethod.getParameterTypes()[1];
        }
        try {
            if (paramType instanceof ParameterizedType)
                return RecordUtil.createSchema(param, (ParameterizedType) paramType);
            else
                return RecordUtil.createSchema(param, null);
        } catch (SchemaExtractionException e) {
            e.printStackTrace();
        }
        return null;
        */

}
