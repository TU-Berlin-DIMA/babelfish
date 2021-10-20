package de.tub.dima.babelfish.storage.layout;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.typesytem.schema.Schema;

public class PhysicalCSVLayout implements PhysicalLayout {

    public final Schema schema;

    public PhysicalCSVLayout(Schema schema) {
        this.schema = schema;
    }

    @Override
    public PhysicalSchema getSchema() {
        return null;
    }

    @Override
    public void initBuffer(Buffer buffer) {

    }

    @Override
    public long getNumberOfRecordsInBuffer(Buffer buffer) {
        return 0;
    }

    @Override
    public long getNumberOfRecordsInBuffer(long virtualAddress) {
        return 0;
    }

    @Override
    public AddressPointer getFieldBufferOffset(long recordIndex, long fieldIndex) {
        return null;
    }

    @Override
    public void incrementRecordNumber(Buffer buffer) {

    }


}
