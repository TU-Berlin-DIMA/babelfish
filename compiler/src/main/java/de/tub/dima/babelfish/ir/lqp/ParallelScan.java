package de.tub.dima.babelfish.ir.lqp;

@Operator(name = "ParallelScan")
public class ParallelScan extends LogicalOperator {
    public final String catalogName;
    public final int threads;
    public final int chunkSize;

    public ParallelScan(String catalogName, int threads, int chunkSize) {
        this.catalogName = catalogName;
        this.threads = threads;
        this.chunkSize = chunkSize;
    }
}
