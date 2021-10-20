package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

public class PointerLongBFArray extends BFArray {

    private final long startAddress;

    public PointerLongBFArray(long startAddress, int length) {
        super(length);
        this.startAddress = startAddress;
    }


    public long getStartAddress() {
        return startAddress;
    }
    @Override
    public ComponentTypes getComponentType() {
        return ComponentTypes.Int_64;
    }

}
