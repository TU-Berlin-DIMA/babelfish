package de.tub.dima.babelfish.ir.instructiongraph.nodes.state;

import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.IterableNodeType;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.calc.IntegerEqualsNode;
import org.graalvm.compiler.nodes.java.UnsafeCompareAndSwapNode;
import org.graalvm.compiler.nodes.spi.Lowerable;
import org.graalvm.compiler.nodes.spi.LoweringTool;

import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_64;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_0;
import static org.graalvm.compiler.nodes.NamedLocationIdentity.OFF_HEAP_LOCATION;


@NodeInfo(cycles = CYCLES_64, size = SIZE_0)
public class TransactionStartNode extends FixedWithNextNode implements Lowerable, IterableNodeType {

    public static final NodeClass<TransactionStartNode> TYPE = NodeClass.create(TransactionStartNode.class);

    @Input
    private ValueNode address;

    public TransactionStartNode(ValueNode address) {
        super(TYPE, StampFactory.forVoid());
        this.address = address;
    }

    private UnsafeCompareAndSwapNode createCompareAndSwap(){
        ValueNode object = ConstantNode.forInt(0);
        ValueNode expected = ConstantNode.forInt(0);
        ValueNode newValue = ConstantNode.forInt(1);
        UnsafeCompareAndSwapNode compAndSwap = new UnsafeCompareAndSwapNode(
                object,
                address,
                expected,
                newValue,
                JavaKind.Int,
                OFF_HEAP_LOCATION
        );
        return graph().addWithoutUniqueWithInputs(compAndSwap);
    }

    @Override
    public void lower(LoweringTool tool) {

        EndNode preLoopEnd = graph().add(new EndNode());
        LoopBeginNode loopBegin = graph().add(new LoopBeginNode());
        FixedNode oldNext = this.next();
        this.setNext(preLoopEnd);
        /* Add the single non-loop predecessor of the loop header. */
        loopBegin.addForwardEnd(preLoopEnd);


        UnsafeCompareAndSwapNode comp = createCompareAndSwap();
        loopBegin.setNext(comp);

        ValueNode zero = ConstantNode.forInt(0);
        LogicNode integerCompare = IntegerEqualsNode.create(comp, zero, NodeView.DEFAULT);

        LoopExitNode loopExit =  graph().add(new LoopExitNode(loopBegin));
        BeginNode ifBeginNode = graph().add(new BeginNode());
        IfNode ifNode = graph().addWithoutUniqueWithInputs(new IfNode(integerCompare, ifBeginNode, loopExit, 0.5));
        comp.setNext(ifNode);
        LoopEndNode loopEndNode =  graph().add(new LoopEndNode(loopBegin));
        PauseNode pauseNode = graph().add(new PauseNode());
        pauseNode.setNext(loopEndNode);
        ifBeginNode.setNext(pauseNode);

        loopExit.setNext(oldNext);

        tool.getLowerer().lower(comp, tool);
        //this.safeDelete();
       // loopBegin.setStateAfter(this.sta);
        this.graph().removeFixed(this);
    }
}
