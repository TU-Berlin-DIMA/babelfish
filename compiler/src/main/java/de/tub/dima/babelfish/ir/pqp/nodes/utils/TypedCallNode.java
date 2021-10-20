package de.tub.dima.babelfish.ir.pqp.nodes.utils;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.RootNode;

public class TypedCallNode<T> extends Node {
    @Child
    private DirectCallNode callNode;

    public TypedCallNode(RootNode rootNode) {
        RootCallTarget callTarget = Truffle.getRuntime().createCallTarget(rootNode);
        callNode = Truffle.getRuntime().createDirectCallNode(callTarget);
    }

    public T call(Object... ags) {
        return (T) callNode.call(ags);
    }

}
