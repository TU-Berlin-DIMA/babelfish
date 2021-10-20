package de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.storage.UnsafeUtils;

/**
 * Simple sink that prints each input record to the console.
 */
@NodeInfo(shortName = "print")
public class BFPrintSink extends BFOperator {


    private final long pointer;
    @CompilerDirectives.CompilationFinal
    private RecordSchema schema;
    @Children
    private PrintLuthTypeNode[] printNodes;

    public BFPrintSink(TruffleLanguage<?> language, FrameDescriptor frameDescriptor) {
        super(language, frameDescriptor);
        pointer = UnsafeUtils.UNSAFE.allocateMemory(100);
    }

    @CompilerDirectives.TruffleBoundary
    public static void print() {
        System.out.println("\n");
    }

    @ExplodeLoop
    private void printAllFields(VirtualFrame localFrame) {
        if (CompilerDirectives.inInterpreter() && printNodes == null) {
            PrintLuthTypeNode[] printNodes = new PrintLuthTypeNode[schema.last];
            for (int i = 0; i < schema.last; i++) {
                String name = schema.fieldNames[i];
                ReadLuthFieldNode readNode = new ReadLuthFieldNode(name, inputObjectSlot);
                printNodes[i] = PrintLuthTypeNodeGen.create(readNode);
            }
            this.printNodes = insert(printNodes);
        }
        for (int i = 0; i < printNodes.length; i++) {
            printNodes[i].execute(localFrame);
        }
        print();
    }

    @Override
    public void execute(VirtualFrame frame) {
        if (CompilerDirectives.inInterpreter() && schema == null) {
            BFRecord value = (BFRecord) frame.getValue(inputObjectSlot);
            schema = value.getObjectSchema();
        }
        printAllFields(frame);
    }
}
