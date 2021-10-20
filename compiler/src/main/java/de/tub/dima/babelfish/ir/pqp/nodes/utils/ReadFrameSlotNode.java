package de.tub.dima.babelfish.ir.pqp.nodes.utils;

import com.oracle.truffle.api.frame.*;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.source.SourceSection;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;

public class ReadFrameSlotNode extends BFExecutableNode {

    private final FrameSlot slot;

    public ReadFrameSlotNode(FrameDescriptor frameDescriptor, String field) {
        this.slot = frameDescriptor.findOrAddFrameSlot(field, FrameSlotKind.Object);
    }

    public ReadFrameSlotNode(FrameSlot frameSlot) {
        this.slot = frameSlot;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        try {
            return frame.getObject(slot);
        } catch (FrameSlotTypeException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean isInstrumentable() {
        return true;
    }

    @Override
    public SourceSection getSourceSection() {
        return Source.newBuilder("test", "test\nbal\nha", "test").build().createSection(0, 10);
    }
}
