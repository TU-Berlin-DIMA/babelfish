package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan;

public final class ChunkContext {
    private final long chunkEnd;
    private long index;

    public ChunkContext(long index, long chunkEnd) {
        this.index = index;
        this.chunkEnd = chunkEnd;
    }

    public boolean hasMore() {
        return index < chunkEnd;
    }

    public long next() {
        return index++;
    }

    public long getChunkEnd() {
        return chunkEnd;
    }

    public long getIndex() {
        return index;
    }
}