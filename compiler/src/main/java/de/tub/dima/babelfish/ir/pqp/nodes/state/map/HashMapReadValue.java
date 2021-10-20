package de.tub.dima.babelfish.ir.pqp.nodes.state.map;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.TypedCallNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.ir.pqp.objects.state.map.DynHashMap;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * Primitive IR node to read a the hash map value as a {@link BFRecord}.
 */
@NodeChild(type = ArgumentReadNode.class)
public abstract class HashMapReadValue extends RootNode {

    private final PhysicalSchema physicalSchema;
    private final RecordSchema recordSchema;

    protected HashMapReadValue(PhysicalSchema physicalSchema) {
        super(null);
        this.physicalSchema = physicalSchema;
        this.recordSchema = new RecordSchema();
        for (PhysicalField<?> field : physicalSchema.getFields()) {
            recordSchema.addField(field.getName());
        }
    }

    public static TypedCallNode<BFRecord> create(PhysicalSchema physicalSchema) {
        HashMapReadValue readNode = HashMapReadValueNodeGen.create(physicalSchema, new ArgumentReadNode(0));
        return new TypedCallNode<>(readNode);
    }

    /**
     * Read a hash map value as {@link BFRecord} from a particular entry address.
     *
     * @param entryAddress
     * @return value as {@link BFRecord}
     */
    @Specialization
    public BFRecord readValue(long entryAddress) {
        BFRecord record = BFRecord.createObject(recordSchema);
        return read(record, entryAddress);
    }

    @ExplodeLoop
    private BFRecord read(BFRecord record, long entryAddress) {
        for (int index = 0; index < physicalSchema.getSize(); index++) {
            PhysicalField<?> field = physicalSchema.getField(index);
            long inBufferOffset = DynHashMap.EntryHandler.getValueAddress(entryAddress) + physicalSchema.getRecordOffset(index);
            BFType value = field.readValue(new AddressPointer(inBufferOffset));
            record.setValue(index, value);
        }
        return record;
    }
}
