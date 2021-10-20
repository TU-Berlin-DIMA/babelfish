package de.tub.dima.babelfish.ir.pqp.nodes.relational.selection;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.profiles.ConditionProfile;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;

/**
 * Relational select operator.
 * Defines a predicate as child, it passes all records for witch the predicate matches.
 */
public class BFSelectionOperator extends BFOperator {

    @Child
    private PredicateNode predicate;

    private final ConditionProfile condition = ConditionProfile.createCountingProfile();


    public BFSelectionOperator(TruffleLanguage<?> language,
                               FrameDescriptor frameDescriptor,
                               PredicateNode predicateNode,
                               BFOperator next) {
        super(language, frameDescriptor, next);
        this.predicate = predicateNode;
    }

    @Override
    public void execute(VirtualFrame frame) {
        boolean res = condition.profile(predicate.executeAsBoolean(frame));
        if (res) {
            callNextExecute(frame.getValue(inputObjectSlot), frame.getValue(stateManagerSlot));
        }
    }
}
