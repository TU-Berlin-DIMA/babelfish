package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

public class PointerIntBFArray extends BFArray {

    private final long startAddress;

    public PointerIntBFArray(long startAddress, int length) {
        super(length);
        this.startAddress = startAddress;
    }

    public long getStartAddress() {
        return startAddress;
    }

    @Override
    public ComponentTypes getComponentType() {
        return ComponentTypes.Int_32;
    }
}
