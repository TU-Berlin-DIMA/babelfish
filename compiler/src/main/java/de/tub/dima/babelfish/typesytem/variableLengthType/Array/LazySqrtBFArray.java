package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

public class LazySqrtBFArray extends BFArray {

    public final BFArray leftSource;

    protected LazySqrtBFArray(BFArray leftSource, int length) {
        super(length);
        this.leftSource = leftSource;
    }

    @Override
    public ComponentTypes getComponentType() {
        return leftSource.getComponentType();
    }

    public BFArray getLeftSource() {
        return leftSource;
    }

}
