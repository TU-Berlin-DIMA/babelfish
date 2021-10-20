package de.tub.dima.babelfish.benchmark.layout;

import de.tub.dima.babelfish.storage.*;
import de.tub.dima.babelfish.storage.layout.PhysicalCSVLayout;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;

import java.util.function.Function;

public class CSVLayoutGenerator {

    public static void generate(long records, Function<Integer, CachlineRecord> function) throws SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        Schema schema = RecordUtil.createSchema(CachlineRecord.class);
        StringBuilder builder = new StringBuilder();
        builder.ensureCapacity((int) records);
        for (int i = 0; i < records; i++) {
            CachlineRecord record = function.apply(i);
            writeToCSVBuffer(record, builder);
            builder.append('\n');
        }

        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(builder.length()));
        long address = buffer.getVirtualAddress().getAddress();
        for (int i = 0; i < builder.length(); i++) {
            UnsafeUtils.putByte(address + i, (byte) builder.charAt(i));
        }
        builder.setLength(0);
        Catalog.getInstance().registerLayout("layout", new PhysicalCSVLayout(schema));
        Catalog.getInstance().registerBuffer("layout", buffer);
    }

    private static void writeToCSVBuffer(CachlineRecord record, StringBuilder buffer) {
        buffer.append(record.field_1);
        buffer.append('|');
        buffer.append(record.field_2);
        buffer.append('|');
        buffer.append(record.field_3);
        buffer.append('|');
        buffer.append(record.field_4);
        buffer.append('|');
        buffer.append(record.field_5);
        buffer.append('|');
        buffer.append(record.field_6);
        buffer.append('|');
        buffer.append(record.field_7);
        buffer.append('|');
        buffer.append(record.field_8);
        buffer.append('|');
        buffer.append(record.field_9);
        buffer.append('|');
        buffer.append(record.field_10);
        buffer.append('|');
        buffer.append(record.field_11);
        buffer.append('|');
        buffer.append(record.field_12);
        buffer.append('|');
        buffer.append(record.field_13);
        buffer.append('|');
        buffer.append(record.field_14);
        buffer.append('|');
        buffer.append(record.field_15);
        buffer.append('|');
        buffer.append(record.field_16);
        buffer.append('|');

    }

}
