package de.tub.dima.babelfish.storage;

/**
 * Abstraction for a single memory buffer.
 * Manages a raw pointer to the off-heap.
 * This memory is not managed by the garbage collector.
 */
public class Buffer {

    private static final Unit.Bytes DEFAULT_ALIGNMENT = new Unit.Bytes(1024);

    private final AddressPointer physicalAddress;
    private final AddressPointer virtualAddress;
    private final Unit.Bytes allocatedSize;
    private final Unit.Bytes size;
    private final Unit.Bytes alignment;

    public Buffer(AddressPointer physicalAddress, Unit.Bytes allocatedSize) {
        this(physicalAddress, allocatedSize, DEFAULT_ALIGNMENT);
    }

    public Buffer(AddressPointer physicalAddress, Unit.Bytes allocatedSize, Unit.Bytes alignment) {
        this.physicalAddress = physicalAddress;
        this.allocatedSize = allocatedSize;
       // this.alignment = alignment;
        this.alignment = new Unit.Bytes(0);
        //this.virtualAddress = alignBuffer(alignment);
        this.virtualAddress = physicalAddress;
        //this.size = new Unit.Bytes(allocatedSize.getBytes() - (virtualAddress.getAddress()-physicalAddress.getAddress()));
        this.size = allocatedSize;
    }

    private AddressPointer alignBuffer(Unit.Bytes alignment){
        return new AddressPointer((physicalAddress.getAddress() & -alignment.getBytes()) + alignment.getBytes());
    }

    public Unit.Bytes getAllocatedSize() {
        return allocatedSize;
    }

    public Unit.Bytes getSize() {
        return size;
    }

    public Unit.Bytes getAlignment() {
        return alignment;
    }

    public AddressPointer getPhysicalAddress() {
        return physicalAddress;
    }

    public AddressPointer getVirtualAddress() {
        return virtualAddress;
    }
}
