package de.tub.dima.babelfish.ir.pqp.objects.state;

public class BFStateContext {

    private static int COUNTER = 0;

    private final int index;

    public BFStateContext() {
        index = COUNTER++;
    }

    public int getIndex() {
        return index;
    }
}
