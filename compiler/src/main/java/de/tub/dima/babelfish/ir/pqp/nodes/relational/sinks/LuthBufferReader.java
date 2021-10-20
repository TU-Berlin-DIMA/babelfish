package de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;

public class LuthBufferReader {

    @CompilerDirectives.CompilationFinal
    private final PhysicalLayout physicalLayout;

    public LuthBufferReader(PhysicalLayout physicalLayout) {
        this.physicalLayout = physicalLayout;
    }

    public ReaderContext createContext(long buffer) {
        long maxNumberOfRecord = physicalLayout.getNumberOfRecordsInBuffer(buffer);
        return new ReaderContext(maxNumberOfRecord, buffer);
    }

    public boolean hasNext(ReaderContext context) {
        return context.currentIndex < context.maxNumberOfRecord;
    }


    @ExplodeLoop
    public void next(ReaderContext context, BFRecord object) {
        context.currentIndex++;
    }

    public PhysicalLayout getPhysicalLayout() {
        return physicalLayout;
    }

    public static class ReaderContext {
        private final long buffer;
        private final long maxNumberOfRecord;
        private long currentIndex = 0L;


        public ReaderContext(long maxNumberOfRecord, long buffer) {
            this.maxNumberOfRecord = maxNumberOfRecord;
            this.buffer = buffer;
        }

        public long getBuffer() {
            return buffer;
        }

        public long getCurrentIndex() {
            return currentIndex;
        }

        public long getMaxNumberOfRecord() {
            return maxNumberOfRecord;
        }
    }
}
