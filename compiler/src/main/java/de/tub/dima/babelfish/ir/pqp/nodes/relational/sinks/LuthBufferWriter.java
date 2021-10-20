package de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalRowLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.storage.layout.fields.PhysicalFieldFactory;
import de.tub.dima.babelfish.typesytem.BFType;

public class LuthBufferWriter {

    @CompilerDirectives.CompilationFinal
    private PhysicalLayout physicalLayout;


    @CompilerDirectives.TruffleBoundary(allowInlining = false)
    private Buffer getNewBuffer(BufferManager bufferManager) {
        return bufferManager.allocateBuffer(new Unit.Bytes(5000));
    }

    private boolean bufferHasSpace() {
        return true;
    }

    private void computePhysicalLayout(BFRecord object) {
        RecordSchema schema = object.getObjectSchema();
        PhysicalSchema.Builder builder = new PhysicalSchema.Builder();
        for (int i = 0; i < schema.last; i++) {
            String fieldName = schema.fieldNames[i];
            Class fieldType = object.getValue(fieldName).getClass();
            PhysicalField physicalField = PhysicalFieldFactory.getPhysicalField(fieldType, fieldName);
            builder.addField(physicalField);
        }
        physicalLayout = new PhysicalRowLayout(builder.build());
    }

    @ExplodeLoop
    private void storeLuthObject(WriterContext context, BFRecord object) {
        for (int i = 0; i < physicalLayout.getSchema().getSize(); i++) {
            PhysicalField field = physicalLayout.getSchema().getField(i);
            AddressPointer inBufferOffset = physicalLayout.getFieldBufferOffset(context.currentIndex, i);
            AddressPointer pointer = new AddressPointer(inBufferOffset.getAddress() + context.buffer);
            BFType value = object.getValue(field.getName());
            field.writeValue(pointer, value);
        }
    }

    public WriterContext createContext(BufferManager bufferManager) {
        Buffer buffer = getNewBuffer(bufferManager);
        if (buffer == null)
            throw new RuntimeException("huck");
        return new WriterContext(buffer);
    }


    public void append(WriterContext context, BFRecord object) {
        if (CompilerDirectives.inInterpreter()) {
            computePhysicalLayout(object);
        }
        storeLuthObject(context, object);
        context.currentIndex++;
    }

    public static class WriterContext {
        private final long buffer;
        private int currentIndex = 0;

        public WriterContext(Buffer buffer) {
            this.buffer = buffer.getVirtualAddress().getAddress();
        }
    }
}
