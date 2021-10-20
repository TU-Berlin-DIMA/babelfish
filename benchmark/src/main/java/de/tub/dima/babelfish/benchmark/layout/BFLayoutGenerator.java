package de.tub.dima.babelfish.benchmark.layout;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.GenericSerializer;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;

import java.util.function.Function;

public class BFLayoutGenerator {

    public static void generate(long records, Function<Integer, CachlineRecord> function, PhysicalLayoutFactory layoutFactory) throws SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        Schema schema = RecordUtil.createSchema(CachlineRecord.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * records) + 1024);
        PhysicalLayout layout = layoutFactory.create(physicalSchema, bufferSize);
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));

        for (int i = 0; i < records; i++) {
            Record record = function.apply(i);
            GenericSerializer.serialize(schema, layout, buffer, record);
        }

        Catalog.getInstance().registerLayout("layout", layout);
        Catalog.getInstance().registerBuffer("layout", buffer);

    }

}
