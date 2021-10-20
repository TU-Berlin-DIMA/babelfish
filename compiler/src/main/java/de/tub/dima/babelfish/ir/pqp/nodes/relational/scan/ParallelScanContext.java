package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan;

import com.oracle.truffle.api.CompilerDirectives;

import java.util.concurrent.atomic.AtomicLong;

public final class ParallelScanContext {
    private final long records;
    private final long chunkSize;
    private AtomicLong nextChunkStart = new AtomicLong(0);


    public ParallelScanContext(long records, long chunkSize) {
        this.records = records;
        this.chunkSize = chunkSize;
    }


    public ChunkContext getNextChunkStart() {
        long currentStart = nextChunkStart.get();
        long current, next;
        do {
            current = nextChunkStart.get();
            next = Math.min(current + chunkSize, records);
        } while (!nextChunkStart.compareAndSet(current, next));

        if (CompilerDirectives.inInterpreter()) {
            System.out.println(Thread.currentThread().getName() + " get chunk " + currentStart + " - " + nextChunkStart);
        }
        return new ChunkContext(currentStart, next);
    }
}