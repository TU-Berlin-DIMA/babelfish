package de.tub.dima.babelfish.ir.pqp.objects;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.CachedContext;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.polyglot.TranslatePolyglotResultNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;

/**
 * Polyglot Context Object.
 * Can be passed to UDFs and defines function to emit records, and to access the state backend.
 */
@ExportLibrary(value = InteropLibrary.class)
public final class BFUDFContext implements TruffleObject {
    /**
     * The next operator we want to call.
     */
    private final BFOperator nextOperator;

    private BFStateManager manager;

    public BFUDFContext(BFOperator next,
                        BFStateManager manager) {
        this.nextOperator = next;
        this.manager = manager;
    }

    public static BFUDFContext createObject(BFOperator next, BFStateManager manager) {
        return new BFUDFContext(next, manager);
    }


    public boolean isAdoptable() {
        return true;
    }


    @ExportMessage
    public static boolean isMemberReadable(BFUDFContext receiver, String inclue) {
        return true;
    }

    @ExportMessage
    public static Object getMembers(BFUDFContext receiver, boolean inclue) {
        return true;
    }

    @ExportMessage
    public static boolean hasMembers(BFUDFContext receiver) {
        return true;
    }

    @ExportMessage
    public static Object readMember(BFUDFContext receiver, String member, @Cached(value = "member") String member_cached) {
        return receiver;
    }

    @ExportMessage
    public static boolean isMemberInvocable(BFUDFContext receiver, String member) {
        return true;
    }

    public static EmitCallNode getInvocationNode(BFUDFContext receiver, String member, TruffleLanguage.ContextReference<BabelfishEngine.BabelfishContext> ctx) {
        switch (member) {
            case "emit": {
                return new EmitCallNode(receiver, ctx.get());
            }
        }
        return null;
    }

    @ExportMessage
    static class InvokeMember {
        @Specialization(guards = "member == c_member")
        static Object invokeMember(BFUDFContext receiver,
                                   String member,
                                   Object[] arguments,
                                   @Cached(value = "member", allowUncached = true) String c_member,
                                   @CachedContext(BabelfishEngine.class) TruffleLanguage.ContextReference<BabelfishEngine.BabelfishContext> ref,
                                   @Cached(value = "getInvocationNode(receiver, member, ref)", allowUncached = true) EmitCallNode emitCallNode) throws UnsupportedMessageException {
            emitCallNode.call(receiver, arguments);
            return 0;
        }
    }


    @ExportMessage
    public boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    public Class<? extends TruffleLanguage<?>> getLanguage(){
        return BabelfishEngine.class;
    }

    @ExportMessage
    Object toDisplayString(boolean allowSideEffects) {
        return null;
    }


    protected static class EmitCallNode extends Node {

        private final FrameDescriptor frameDescriptor;
        private final FrameSlot polyglotObjectSlot;
        private final FrameSlot resultObjectSlot;
        @Child
        private TranslatePolyglotResultNode translatePolyglotResultNode;

        @Child
        public DirectCallNode nextExecute;

        public EmitCallNode(BFUDFContext receiver, BabelfishEngine.BabelfishContext ctx) {
            RootNode executeCall = receiver.nextOperator.getExecuteCall();
            RootCallTarget callTarget = Truffle.getRuntime().createCallTarget(executeCall);
            nextExecute = Truffle.getRuntime().createDirectCallNode(callTarget);
            frameDescriptor = new FrameDescriptor();
            polyglotObjectSlot = frameDescriptor.findOrAddFrameSlot("polyglotObject", FrameSlotKind.Object);
            resultObjectSlot = frameDescriptor.findOrAddFrameSlot("resultObject", FrameSlotKind.Object);
            translatePolyglotResultNode = TranslatePolyglotResultNode.create(ctx, frameDescriptor, polyglotObjectSlot, resultObjectSlot);
        }

        public void call(BFUDFContext receiver, Object[] arguments) {
            Object polyglotObject = arguments[0];
            VirtualFrame newFrame = Truffle.getRuntime().createVirtualFrame(new Object[0], frameDescriptor);
            newFrame.setObject(polyglotObjectSlot, polyglotObject);

            BFRecord resultObejct = translatePolyglotResultNode.executeAsLuthObject(newFrame);

            nextExecute.call(resultObejct, receiver.manager);
        }
    }
}

