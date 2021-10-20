package de.tub.dima.babelfish.ir.lqp;

public class Sink extends LogicalOperator {


    @Operator(name = "PrintSink")
    public static class PrintSink extends Sink {

    }

    @Operator(name = "MemorySink")
    public static class MemorySink extends Sink {

    }
}
