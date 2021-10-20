package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.profiles.ConditionProfile;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.LuthPolyglotBuiltins;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ReadFrameSlotNode;

/**
 * A UDF based selection operator.
 * This operator expects a return scalar UDF that returns a boolean or an integer of 0 or 1.
 */
public class BFSelectionUDFOperator extends BFScalarUDFOperator {

    @Child
    private BFExecutableNode asBooleanNode;
    private final ConditionProfile condition = ConditionProfile.createCountingProfile();

    public BFSelectionUDFOperator(TruffleLanguage<?> language,
                                  FrameDescriptor frameDescriptor,
                                  BabelfishEngine.BabelfishContext ctx,
                                  PolyglotUDFOperator udf,
                                  BFOperator next) {
        super(language, frameDescriptor, ctx, udf, next);
        asBooleanNode = LuthPolyglotBuiltins.createAsBoolean(ctx,
                new ReadFrameSlotNode(resultFrameSlot)
        );
    }

    @Override
    public void execute(VirtualFrame frame) {
        Object udfResult = callScalarUDF(frame);
        frame.setObject(resultFrameSlot, udfResult);
        boolean udfResultAsBoolean = asBooleanNode.executeAsBoolean(frame);
        if (condition.profile(udfResultAsBoolean)) {
            callNextExecute(frame.getArguments()[0], frame.getArguments()[1]);
        }
    }
}
