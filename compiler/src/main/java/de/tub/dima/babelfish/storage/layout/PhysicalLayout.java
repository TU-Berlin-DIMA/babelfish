package de.tub.dima.babelfish.storage.layout;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.Buffer;

public interface PhysicalLayout {

    PhysicalSchema getSchema();

    public void initBuffer(Buffer buffer);

    public long getNumberOfRecordsInBuffer(Buffer buffer);
    public long getNumberOfRecordsInBuffer(long virtualAddress);

    public AddressPointer getFieldBufferOffset(long recordIndex, long fieldIndex);
    public void incrementRecordNumber(Buffer buffer);

}
