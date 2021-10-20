package de.tub.dima.babelfish.ir.pqp.objects;

import com.oracle.truffle.api.*;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.*;
import de.tub.dima.babelfish.BabelfishEngine;

@ExportLibrary(value = InteropLibrary.class)
public class ExecutableQuery implements TruffleObject {

    private final RootCallTarget callTarget;

    public ExecutableQuery(RootNode pipelineRootNode) {
        callTarget = Truffle.getRuntime().createCallTarget(pipelineRootNode);
    }

    @ExportMessage
    public boolean isExecutable(){
        return true;
    }

    public static DirectCallNode getCallNode(ExecutableQuery obj){
        DirectCallNode target = Truffle.getRuntime().createDirectCallNode(obj.callTarget);
        target.forceInlining();;
        return target;
    }

    @ExportMessage
    public static Object execute(ExecutableQuery obj, Object[] args, @Cached(value = "getCallNode(obj)", allowUncached = true) DirectCallNode node){
        return node.call(args);
    }

    @ExportMessage
    public static boolean hasLanguage(ExecutableQuery obj) {
        return true;
    }

    @ExportMessage
    public static Class<? extends TruffleLanguage<?>> getLanguage(ExecutableQuery obj){
        return BabelfishEngine.class;
    }
    @ExportMessage
    public static  Object toDisplayString(ExecutableQuery obj, boolean allowSideEffects) { return ""; }

}
