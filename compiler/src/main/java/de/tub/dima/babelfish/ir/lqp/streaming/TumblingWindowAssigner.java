package de.tub.dima.babelfish.ir.lqp.streaming;

public class TumblingWindowAssigner implements WindowAssigner {

    private final int size;

    public TumblingWindowAssigner(int size) {
        this.size = size;
    }
}
