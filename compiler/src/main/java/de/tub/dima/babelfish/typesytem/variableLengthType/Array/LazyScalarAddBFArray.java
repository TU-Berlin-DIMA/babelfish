package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

public class LazyScalarAddBFArray extends BFArray {


    public final BFArray leftSource;
    public final Double rightSource;

    protected LazyScalarAddBFArray(BFArray leftSource, Double rightSource, int length) {
        super(length);
        this.leftSource = leftSource;
        this.rightSource = rightSource;
    }

    @Override
    public ComponentTypes getComponentType() {
        return leftSource.getComponentType();
    }

    public BFArray getLeftSource() {
        return leftSource;
    }

    public Double getRightSource() {
        return rightSource;
    }
}
