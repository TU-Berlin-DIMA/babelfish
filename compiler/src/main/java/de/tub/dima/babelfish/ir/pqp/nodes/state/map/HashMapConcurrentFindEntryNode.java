package de.tub.dima.babelfish.ir.pqp.nodes.state.map;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.pqp.nodes.records.BFRecordProjectFieldsNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.EqualBFRecordNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.WriteLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.nodes.state.map.hash.HashBFRecordNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.TypedCallNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.map.DynHashMap;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;

@NodeChild(value = "hashmap", type = ArgumentReadNode.class)
@NodeChild(value = "object", type = ArgumentReadNode.class)
public abstract class HashMapConcurrentFindEntryNode extends RootNode {

    private final FrameSlot inputObjectSlot;
    private final FrameSlot stateObjectSlot;
    private final FrameSlot valueObjectSlot;
    @Child
    private TypedCallNode<Void> writeValueNode;
    @Child
    private TypedCallNode<BFRecord> extractKeyFromState;

    @Child
    private TypedCallNode<Long> getHashValueNode;

    @Child
    private TypedCallNode<BFRecord> readValueNode;

    @Child
    private TypedCallNode<BFRecord> extractKeyFromInput;

    @Child
    private TypedCallNode<Boolean> isEqualNode;

    @Children
    private WriteLuthFieldNode[] keyWriteNodes;

    @Children
    private ReadLuthFieldNode[] keyReadNodes;

    protected HashMapConcurrentFindEntryNode(PhysicalSchema physicalSchema, FieldReference[] keys) {
        super(null, new FrameDescriptor());

        readValueNode = HashMapReadValue.create(physicalSchema);
        writeValueNode = HashMapWriteValue.create(physicalSchema);
        isEqualNode = EqualBFRecordNode.create();
        extractKeyFromInput = new TypedCallNode(BFRecordProjectFieldsNode.create(keys));
        extractKeyFromState = new TypedCallNode(BFRecordProjectFieldsNode.create(keys));
        getHashValueNode = HashBFRecordNode.create();
        keyWriteNodes = new WriteLuthFieldNode[keys.length];
        keyReadNodes = new ReadLuthFieldNode[keys.length];
        inputObjectSlot = getFrameDescriptor().findOrAddFrameSlot("inputObject");
        stateObjectSlot = getFrameDescriptor().findOrAddFrameSlot("stateObject");
        valueObjectSlot = getFrameDescriptor().findOrAddFrameSlot("value");
        for (int i = 0; i < keys.length; i++) {
            keyWriteNodes[i] = new WriteLuthFieldNode(keys[i].getName(), stateObjectSlot, valueObjectSlot);
            keyReadNodes[i] = new ReadLuthFieldNode(keys[i].getName(), inputObjectSlot);
        }

    }

    public static TypedCallNode<Long> create(PhysicalSchema physicalSchema, FieldReference[] keys) {
        return new TypedCallNode(HashMapConcurrentFindEntryNodeGen.create(physicalSchema, keys, new ArgumentReadNode(0),
                new ArgumentReadNode(1)));
    }

    @ExplodeLoop
    private BFRecord copyKeyValuesToState(BFRecord keyObject, long entry) {

        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(null, getFrameDescriptor());
        frame.setObject(inputObjectSlot, keyObject);
        BFRecord currentValue = readValueNode.call(entry);
        frame.setObject(stateObjectSlot, currentValue);
        for (int i = 0; i < keyReadNodes.length; i++) {
            Object keyValue = keyReadNodes[i].execute(frame);
            // if (CompilerDirectives.inInterpreter())
            //     System.out.println("new key" + keyValue);
            frame.setObject(valueObjectSlot, keyValue);
            keyWriteNodes[i].execute(frame);
        }

        return currentValue;
    }

    long getKeyAddress(long bufferAddress, long keyBufferOffset, long position) {
        return bufferAddress + keyBufferOffset + (position * 8);
    }

    public long getEntry(long bufferAddress, long keyBufferOffset, long position) {
        long keyAddress = getKeyAddress(bufferAddress, keyBufferOffset, position);
        return UnsafeUtils.UNSAFE.getLong(keyAddress);
    }

    @Specialization
    public long findEntry(DynHashMap hashMap,
                          BFRecord object,
                          @Cached(value = "hashMap.getMask()", allowUncached = true) long mask,
                          @Cached(value = "hashMap.getEntrySize()", allowUncached = true) long entrySize,
                          @Cached(value = "hashMap.getEntryBufferOffset()", allowUncached = true) long entryBufferOffset,
                          @Cached(value = "hashMap.getKeyBufferOffset()", allowUncached = true) long keyBufferOffset) {

        long bufferAddress = hashMap.getBufferAddress();
        BFRecord keyObject = extractKeyFromInput.call(object);
        long hash = getHashValueNode.call(keyObject);

        long pos = hash & mask;
        long entry = getEntry(bufferAddress, keyBufferOffset, pos);

        // check if hashmap already contains an entry for this key
        entry = checkEntry(entry, keyObject);
        if (entry != 0) {
            return entry;
        }
        // hashmap contains no entry -> we have to add a new one
        long nextEntryAddress = getNextEntryAtomic(bufferAddress, entrySize, entryBufferOffset);
        while (entry == 0) {
            long keyAddress = getKeyAddress(bufferAddress, keyBufferOffset, pos);
            long oldEntryAddress = UnsafeUtils.getLong(keyAddress);
            DynHashMap.EntryHandler.setHash(nextEntryAddress, 0);
            DynHashMap.EntryHandler.setNext(nextEntryAddress, oldEntryAddress);
            if (!UnsafeUtils.UNSAFE.compareAndSwapLong(null, keyAddress, oldEntryAddress, nextEntryAddress)) {
                entry = getEntry(bufferAddress, keyBufferOffset, pos);
                entry = checkEntry(entry, keyObject);
                if (entry != 0) {
                    return entry;
                }

            }
            entry = nextEntryAddress;
        }

        //System.out.println("new key "+ keyObject.getValue(0));
        //hashMap.print();
        BFRecord newState = copyKeyValuesToState(keyObject, entry);
        writeValueNode.call(newState, entry);
        return entry;
    }

    public long getNextEntryAtomic(long bufferAddress, long entrySize, long entryBufferOffset) {
        // increase the max current entry index by one and return old index
        long currentEntryIndex = UnsafeUtils.UNSAFE.getAndAddLong(null, bufferAddress, 1);
        return bufferAddress + entryBufferOffset + (entrySize * currentEntryIndex);
    }


    public long checkEntry(long entry, BFRecord keyObject) {
        for (; entry != 0; entry = DynHashMap.EntryHandler.getNext(entry)) {
            BFRecord hashMapValue = readValueNode.call(entry);
            BFRecord keyValue = extractKeyFromState.call(hashMapValue);
            if (isEqualNode.call(keyValue, keyObject)) {
                // we found the entry
                return entry;
            }
        }
        return 0;
    }


}
