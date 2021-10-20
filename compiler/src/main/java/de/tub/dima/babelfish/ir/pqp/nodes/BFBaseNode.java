package de.tub.dima.babelfish.ir.pqp.nodes;


import com.oracle.truffle.api.dsl.Introspectable;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(description = "The abstract base node for all luth nodes")
@Introspectable
public abstract class BFBaseNode extends Node {
}
