package de.tub.dima.babelfish.ir.instructiongraph.nodes.state;

import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.IterableNodeType;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.FixedWithNextNode;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.java.UnsafeCompareAndSwapNode;
import org.graalvm.compiler.nodes.spi.Lowerable;
import org.graalvm.compiler.nodes.spi.LoweringTool;

import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_64;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_0;
import static org.graalvm.compiler.nodes.NamedLocationIdentity.OFF_HEAP_LOCATION;


@NodeInfo(cycles = CYCLES_64, size = SIZE_0)
public class TransactionEndNode extends FixedWithNextNode implements Lowerable, IterableNodeType {

    public static final NodeClass<TransactionEndNode> TYPE = NodeClass.create(TransactionEndNode.class);

    @Input
    private ValueNode address;

    public TransactionEndNode(ValueNode address) {
        super(TYPE, StampFactory.forVoid());
        this.address = address;
    }

    @Override
    public void lower(LoweringTool tool) {
        ValueNode object = ConstantNode.forInt(0);
        ValueNode expected = ConstantNode.forInt(1);
        ValueNode newValue = ConstantNode.forInt(0);
        UnsafeCompareAndSwapNode compAndSwap = new UnsafeCompareAndSwapNode(
                object,
                address,
                expected,
                newValue,
                JavaKind.Int,
                OFF_HEAP_LOCATION
        );
        compAndSwap = graph().addWithoutUniqueWithInputs(compAndSwap);
        graph().replaceFixedWithFixed(this, compAndSwap);
        tool.getLowerer().lower(compAndSwap, tool);
    }
}
