package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.LuthPolyglotBuiltins;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ConstantLuthExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ReadFrameSlotNode;

/**
 * Primitive to convert a polyglot TruffleObject to a BFRecord
 * We distinguish between Object and ArrayShapes for polyglot records.
 */
@NodeInfo(shortName = "Copy #name to luth object")
public class ConvertPolyglotRecordsToBFRecord extends BFExecutableNode {

    private final FrameSlot tempFieldValueSlot;
    @Child
    private BFExecutableNode readTruffleObjectNode;
    @Child
    private BFExecutableNode writeLuthObjectNode;

    public ConvertPolyglotRecordsToBFRecord(BabelfishEngine.BabelfishContext ctx,
                                            FrameDescriptor frameDescriptor,
                                            BFExecutableNode getFieldNode,
                                            FrameSlot luthObjectSlot,
                                            String fieldName) {
        readTruffleObjectNode = getFieldNode;
        writeLuthObjectNode = LuthPolyglotBuiltins.createSetNode(ctx,
                new BFExecutableNode[]{new ReadFrameSlotNode(luthObjectSlot),
                        new ReadFrameSlotNode(frameDescriptor, fieldName + "_temp"),
                        new ConstantLuthExecutableNode(fieldName)});
        tempFieldValueSlot = frameDescriptor.findOrAddFrameSlot(fieldName + "_temp", FrameSlotKind.Object);
    }

    public static BFExecutableNode create(BabelfishEngine.BabelfishContext ctx,
                                          FrameDescriptor frameDescriptor,
                                          FrameSlot polyglotObjectSlot,
                                          FrameSlot luthObjectSlot,
                                          String fieldName) {
        BFExecutableNode getFieldNode = LuthPolyglotBuiltins.createGetNode(ctx,
                new BFExecutableNode[]{new ReadFrameSlotNode(polyglotObjectSlot),
                        new ConstantLuthExecutableNode(fieldName)});
        return new ConvertPolyglotRecordsToBFRecord(ctx, frameDescriptor, getFieldNode, luthObjectSlot, fieldName);
    }

    public static BFExecutableNode create(BabelfishEngine.BabelfishContext ctx,
                                          FrameDescriptor frameDescriptor,
                                          FrameSlot polyglotObjectSlot,
                                          FrameSlot luthObjectSlot,
                                          int fieldIndex) {
        BFExecutableNode readArrayElementNode = LuthPolyglotBuiltins.createReadArrayElementNode(ctx,
                new BFExecutableNode[]{new ReadFrameSlotNode(polyglotObjectSlot),
                        new ConstantLuthExecutableNode(new Long(fieldIndex))});
        return new ConvertPolyglotRecordsToBFRecord(ctx, frameDescriptor, readArrayElementNode, luthObjectSlot, fieldIndex + "_field");
    }

    @Override
    public Object execute(VirtualFrame frame) {
        Object value = readTruffleObjectNode.execute(frame);
        frame.setObject(tempFieldValueSlot, value);
        writeLuthObjectNode.execute(frame);
        return null;
    }

    @Override
    public boolean isInstrumentable() {
        return true;
    }
}
