package de.tub.dima.babelfish.storage.layout;

import com.oracle.truffle.api.*;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.UnsafeUtils;

public class PhysicalColumnLayout implements PhysicalLayout {

    public static boolean OPT_CACHE_SET_COLLISION = false;

    @CompilerDirectives.CompilationFinal()
    public final PhysicalSchema physicalSchema;

    private static final Unit.Bytes RECORD_NUMBER_OFFSET = new Unit.Bytes(0);
    private static final Unit.Bytes DATA_OFFSET = new Unit.Bytes(64);
    @CompilerDirectives.CompilationFinal(dimensions=1)
    private final long[] columnOffsets;

    public PhysicalColumnLayout(PhysicalSchema physicalSchema, long targetBufferSize) {
        this.physicalSchema = physicalSchema;
        this.columnOffsets = new long[physicalSchema.getFields().length];
        long maxRecordsInBuffer = targetBufferSize / physicalSchema.getFixedRecordSize();
        long currentOffset = 0;
        for (int columnIndex = 0; columnIndex < columnOffsets.length; columnIndex++) {
            columnOffsets[columnIndex] = currentOffset;
            currentOffset = columnOffsets[columnIndex]
                    + addOffset(columnIndex)
                    + (physicalSchema.getFields()[columnIndex].getPhysicalSize() * maxRecordsInBuffer);
        }
        System.out.println("max" + maxRecordsInBuffer);
    }

    public long addOffset(int columnIndex){
        if(!OPT_CACHE_SET_COLLISION)
            return 0;
        return 64 * (columnIndex % 64);
    }

    public void initBuffer(Buffer buffer) {
        buffer.getVirtualAddress().add(RECORD_NUMBER_OFFSET).putLong(0);
    }

    public long getNumberOfRecordsInBuffer(Buffer buffer) {
        return buffer.getVirtualAddress().add(RECORD_NUMBER_OFFSET).getLong();
    }

    public long getNumberOfRecordsInBuffer(long virtualAddress) {
        return UnsafeUtils.getLong(virtualAddress + RECORD_NUMBER_OFFSET.getBytes());
    }


    public AddressPointer getFieldBufferOffset(long recordIndex, long fieldIndex) {
        return new AddressPointer(DATA_OFFSET.getBytes() + columnOffsets[(int) fieldIndex] + (physicalSchema.getField(fieldIndex).getPhysicalSize() * recordIndex));
    }

    @Override
    public PhysicalSchema getSchema() {
        return physicalSchema;
    }

    public void incrementRecordNumber(Buffer buffer) {
        buffer.getVirtualAddress().add(RECORD_NUMBER_OFFSET).putLong(getNumberOfRecordsInBuffer(buffer) + 1);
    }

    public void incrementRecordNumber(long virtualAddress) {
        UnsafeUtils.putLong(virtualAddress + RECORD_NUMBER_OFFSET.getBytes(),getNumberOfRecordsInBuffer(virtualAddress) + 1);
    }


}
