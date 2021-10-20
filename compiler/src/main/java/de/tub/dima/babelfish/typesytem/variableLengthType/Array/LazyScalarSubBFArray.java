package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

public class LazyScalarSubBFArray extends BFArray {


    public final Double leftSource;
    public final BFArray rightSource;

    protected LazyScalarSubBFArray(Double leftSource, BFArray rightSource, int length) {
        super(length);
        this.leftSource = leftSource;
        this.rightSource = rightSource;
    }

    @Override
    public ComponentTypes getComponentType() {
        return rightSource.getComponentType();
    }

    public Double getLeftSource() {
        return leftSource;
    }

    public BFArray getRightSource() {
        return rightSource;
    }
}
