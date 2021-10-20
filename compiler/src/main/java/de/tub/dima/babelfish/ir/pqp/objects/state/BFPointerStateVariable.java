package de.tub.dima.babelfish.ir.pqp.objects.state;

public class BFPointerStateVariable implements BFStateVariable {

    private final long address;

    public BFPointerStateVariable(long address) {
        this.address = address;
    }

    public long getAddress() {
        return address;
    }
}
