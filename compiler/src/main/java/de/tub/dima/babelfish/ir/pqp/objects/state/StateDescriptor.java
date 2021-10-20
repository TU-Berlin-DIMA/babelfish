package de.tub.dima.babelfish.ir.pqp.objects.state;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;

import java.util.ArrayList;

public class StateDescriptor {

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private final BFType[]  defaultValue;
    private final PhysicalSchema physicalSchema;
    private final RecordSchema schema;
    private final String name;


    public static class Builder {

        PhysicalSchema.Builder schemaBuilder;
        ArrayList<BFType> defaultValues;

        public Builder() {
            schemaBuilder = new PhysicalSchema.Builder();
            defaultValues = new ArrayList<>();
        }

        public Builder add(PhysicalField physicalField, BFType defaultValue) {
            schemaBuilder.addField(physicalField);
            defaultValues.add(defaultValue);
            return this;
        }

        public StateDescriptor build(String name) {
            return new StateDescriptor(name, schemaBuilder.build(true), defaultValues.toArray(new BFType[defaultValues.size()]));
        }
    }


    public StateDescriptor(String name, PhysicalSchema physicalSchema, BFType[] defaultValues) {
        this.defaultValue = defaultValues;
        this.physicalSchema = physicalSchema;
        this.name  = name;
        schema = new RecordSchema();
        for (int i = 0; i < physicalSchema.getSize(); i++) {
            String fn = physicalSchema.getField(i).getName();
            schema.addField(fn);
        }
    }

    public BFType[] getDefaultValue() {
        return defaultValue;
    }

    public PhysicalSchema getPhysicalSchema() {
        return physicalSchema;
    }

    public long getPhysicalSize() {
        return physicalSchema.getFixedRecordSize();
    }

    public RecordSchema getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }
}
