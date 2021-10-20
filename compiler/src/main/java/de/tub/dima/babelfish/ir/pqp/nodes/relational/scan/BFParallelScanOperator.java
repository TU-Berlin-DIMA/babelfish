package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperatorInterface;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;

import java.util.concurrent.*;

@NodeInfo(shortName = "scan")
public class BFParallelScanOperator extends Node implements BFOperatorInterface {


    @CompilerDirectives.CompilationFinal
    private final ExecutorService threadPool;

    @CompilerDirectives.CompilationFinal
    private final String tableName;

    @CompilerDirectives.CompilationFinal
    private final PhysicalLayout physicalLayout;
    private final int chunkSize;
    private final int threadCount;
    private final FrameSlot counter;
    @Child
    private DirectCallNode scanOperator;
    @Child
    private DirectCallNode openCallNode;

    @Child
    private DirectCallNode closeCallNode;

    public BFParallelScanOperator(TruffleLanguage<?> language,
                                  FrameDescriptor frameDescriptor,
                                  BFOperator child,
                                  String tableName,
                                  int threads,
                                  int chunkSize) {
        this.tableName = tableName;
        this.threadCount = threads;
        this.chunkSize = chunkSize;
        this.counter = frameDescriptor.findOrAddFrameSlot(STATE_MANAGER_FRAME_SLOT_IDENTIFIER, FrameSlotKind.Object);

        int i = 0;
        threadPool = Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r) {
                    @Override
                    public void interrupt() {
                        super.interrupt();
                        System.out.println("Thread is interrupted");
                    }


                };
                t.setDaemon(true);
                //t.setName("Worker Thread Scan");
                return t;
            }

        });
        BFChunkedScan scan = new BFChunkedScan(language, frameDescriptor, tableName, child);
        TruffleRuntime runtime = Truffle.getRuntime();
        this.scanOperator = runtime.createDirectCallNode(runtime.createCallTarget(new RootNode(language) {
            @Override
            public Object execute(VirtualFrame frame) {
                scan.execute(frame);
                return null;
            }
        }));
        this.scanOperator.forceInlining();
        this.physicalLayout = Catalog.getInstance().getLayout(tableName);
        this.openCallNode = runtime.createDirectCallNode(
                runtime.createCallTarget(
                        child.getOpenCall()));

        this.closeCallNode = runtime.createDirectCallNode(
                runtime.createCallTarget(
                        child.getCloseCall()));
    }

    @CompilerDirectives.TruffleBoundary(transferToInterpreterOnException = false)
    public Buffer getBuffer(String layout) {
        return Catalog.getInstance().getBuffer(layout);
    }

    @CompilerDirectives.TruffleBoundary
    public void executeTask(Object stateManager, long bufferAddress, ParallelScanContext scanContext) {
        Future[] f = new Future[threadCount];
        for (int i = 0; i < threadCount; i++) {
            f[i] = threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    scanOperator.call(stateManager, bufferAddress, scanContext);
                    //System.out.println(Thread.currentThread().getName() + " " + duration);
                }
            });
        }
        for (int i = 0; i < threadCount; i++) {
            try {
                //  f[i].get();
                f[i].get();

                //     System.out.println("Tastk:" + i + " terminated");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

           /*

        Thread[] f = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            f[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    scanOperator.call(stateManager, bufferAddress, scanContext);
                }
            });

        f[i].start();

        }

        for (int i = 0; i < threadCount; i++) {
            try {
                f[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

            */

    }


    @Override
    public void execute(VirtualFrame frame) {
        Object stateManager = frame.getValue(counter);

        Buffer buffer = getBuffer(tableName);
        long bufferAddress = buffer.getVirtualAddress().getAddress();
        ParallelScanContext scanContext = new ParallelScanContext(physicalLayout.getNumberOfRecordsInBuffer(buffer), chunkSize);
        executeTask(stateManager, bufferAddress, scanContext);
    }

    @Override
    public void open(VirtualFrame frame) {
        openCallNode.call(frame.getValue(counter));
    }

    @Override
    public void close(VirtualFrame frame) {
        closeCallNode.call(frame.getValue(counter));
    }
}
