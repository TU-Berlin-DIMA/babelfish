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
 * Primitive IR node to write a {@link BFRecord} the a entry of a hash map.
 */
@NodeChild(type = ArgumentReadNode.class)
@NodeChild(type = ArgumentReadNode.class)
public abstract class HashMapWriteValue extends RootNode {

    private final PhysicalSchema physicalSchema;
    private final RecordSchema recordSchema;

    protected HashMapWriteValue(PhysicalSchema physicalSchema) {
        super(null);
        this.physicalSchema = physicalSchema;
        this.recordSchema = new RecordSchema();
        for (PhysicalField<?> field : physicalSchema.getFields()) {
            recordSchema.addField(field.getName());
        }
    }

    public static TypedCallNode<Void> create(PhysicalSchema physicalSchema) {
        HashMapWriteValue writeNode = HashMapWriteValueNodeGen.create(physicalSchema,
                new ArgumentReadNode(0), new ArgumentReadNode(1));
        return new TypedCallNode<>(writeNode);
    }

    @Specialization
    public Object writeLuthObject(BFRecord object, long entry) {
        write(object, entry);
        return null;
    }

    @ExplodeLoop
    private void write(BFRecord object, long address) {
        for (int index = 0; index < physicalSchema.getSize(); index++) {
            PhysicalField field = physicalSchema.getField(index);
            long inBufferOffset = DynHashMap.EntryHandler.getValueAddress(address) + physicalSchema.getRecordOffset(index);
            BFType value = object.getValue(index);
            field.writeValue(new AddressPointer(inBufferOffset), value);
        }
    }
}
