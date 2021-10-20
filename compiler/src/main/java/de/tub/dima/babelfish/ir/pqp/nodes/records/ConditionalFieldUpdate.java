package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.selection.PredicateNode;

public class ConditionalFieldUpdate extends BFExecutableNode {

    private final FrameSlot inputObjectSlot;
    private final FrameSlot valueObjectSlot;
    private final FrameDescriptor frameDescriptor;
    @Child
    private PredicateNode predicate;

    @Child
    private BFExecutableNode trueExpression;

    @Child
    private BFExecutableNode elseExpression;

    @Child
    private WriteLuthFieldNode fieldUpdate;

    public ConditionalFieldUpdate(FrameDescriptor frameDescriptor,
                                  PredicateNode predicate,
                                  BFExecutableNode trueExpression,
                                  BFExecutableNode elseExpression,
                                  String member) {
        this.predicate = predicate;
        this.trueExpression = trueExpression;
        this.elseExpression = elseExpression;
        this.frameDescriptor = frameDescriptor;
        inputObjectSlot = frameDescriptor.findOrAddFrameSlot("object");
        valueObjectSlot = frameDescriptor.findOrAddFrameSlot("valueObject");
        this.fieldUpdate = new WriteLuthFieldNode(member, inputObjectSlot, valueObjectSlot);
    }


    @Override
    public Object execute(VirtualFrame frame) {
        if (predicate.executeAsBoolean(frame)) {
            Object expressionResult = trueExpression.execute(frame);
            frame.setObject(this.valueObjectSlot, expressionResult);
            fieldUpdate.execute(frame);
            return expressionResult;
        } else {
            Object expressionResult = elseExpression.execute(frame);
            frame.setObject(this.valueObjectSlot, expressionResult);
            fieldUpdate.execute(frame);
            return expressionResult;
        }
    }
}
