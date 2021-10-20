package de.tub.dima.babelfish.storage.layout;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.UnsafeUtils;

public class PhysicalRowLayout implements PhysicalLayout {

    public final PhysicalSchema physicalSchema;

    private static final Unit.Bytes RECORD_NUMBER_OFFSET = new Unit.Bytes(0);
    private static final Unit.Bytes DATA_OFFSET = new Unit.Bytes(64);

    public PhysicalRowLayout(PhysicalSchema physicalSchema) {
        this.physicalSchema = physicalSchema;
    }

    public void initBuffer(Buffer buffer){
        buffer.getVirtualAddress().add(RECORD_NUMBER_OFFSET).putLong(0);
    }

    public long getNumberOfRecordsInBuffer(Buffer buffer){
        return buffer.getVirtualAddress().add(RECORD_NUMBER_OFFSET).getLong();
    }

    @Override
    public long getNumberOfRecordsInBuffer(long virtualAddress) {
        return UnsafeUtils.getLong(virtualAddress + RECORD_NUMBER_OFFSET.getBytes());
    }

    public AddressPointer getFieldBufferOffset(long recordIndex, long fieldIndex) {
       return new AddressPointer(DATA_OFFSET.getBytes() + physicalSchema.getRecordOffset(fieldIndex) + (physicalSchema.getFixedRecordSize() * recordIndex));
    }

    @Override
    public PhysicalSchema getSchema() {
        return physicalSchema;
    }

    public void incrementRecordNumber(Buffer buffer) {
        buffer.getVirtualAddress().add(RECORD_NUMBER_OFFSET).putLong(getNumberOfRecordsInBuffer(buffer)+1);
    }

}
