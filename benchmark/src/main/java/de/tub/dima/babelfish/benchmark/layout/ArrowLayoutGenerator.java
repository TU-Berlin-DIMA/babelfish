package de.tub.dima.babelfish.benchmark.layout;

import de.tub.dima.babelfish.benchmark.parser.ArrowCSVImporter;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ArrowLayoutGenerator {

    public static void generate(int records, Function<Integer, CachlineRecord> function) throws SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        RootAllocator allocator = new RootAllocator();
        Schema schema = RecordUtil.createSchema(CachlineRecord.class);
        List<FieldVector> fields = ArrowCSVImporter.getFields(schema, allocator);
        for (int i = 0; i < records; i++) {
            for (int f = 0; f < fields.size(); f++) {
                DecimalVector intVector = (DecimalVector) fields.get(f);
                intVector.setSafe(i, i);
            }
        }
        List<Field> arrowFields = new ArrayList<>();
        for (FieldVector fieldVector : fields) {
            arrowFields.add(fieldVector.getField());
            fieldVector.setValueCount(records);
        }

        VectorSchemaRoot schemaRoot = new VectorSchemaRoot(arrowFields, fields);
        schemaRoot.setRowCount(records);

        Catalog.getInstance().registerArrowLayout("layout", schemaRoot);
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
