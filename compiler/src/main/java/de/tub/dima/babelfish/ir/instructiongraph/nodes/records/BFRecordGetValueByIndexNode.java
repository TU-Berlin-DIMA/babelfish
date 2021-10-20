package de.tub.dima.babelfish.ir.instructiongraph.nodes.records;

import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.graph.spi.Canonicalizable;
import org.graalvm.compiler.graph.spi.CanonicalizerTool;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.FixedWithNextNode;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.graphbuilderconf.InvocationPlugin;
import org.graalvm.compiler.nodes.spi.Virtualizable;
import org.graalvm.compiler.nodes.spi.VirtualizerTool;
import org.graalvm.compiler.nodes.virtual.VirtualObjectNode;

import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_0;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_0;

@NodeInfo(cycles = CYCLES_0, size = SIZE_0)
public final class BFRecordGetValueByIndexNode extends FixedWithNextNode implements Virtualizable, Canonicalizable {

    public static final NodeClass<BFRecordGetValueByIndexNode> TYPE = NodeClass.create(BFRecordGetValueByIndexNode.class);
    @Input
    private ValueNode luthObject;
    @Input
    private ValueNode index;


    public BFRecordGetValueByIndexNode(InvocationPlugin.Receiver luthObjectReceiver, ValueNode index, JavaKind kind) {
        super(TYPE, StampFactory.forKind(kind));
        luthObject = luthObjectReceiver.get(false);
        this.index = index;
    }

    @Override
    public void virtualize(VirtualizerTool tool) {
        if (this.luthObject instanceof NewBFRecordNode) {
            NewBFRecordNode object = (NewBFRecordNode) luthObject;
            // Partial Escape finally removed frame
            ValueNode dataAlias = tool.getAlias(object.virtualFrameObjectArray);
            if (dataAlias instanceof VirtualObjectNode) {
                VirtualObjectNode dataVirtual = (VirtualObjectNode) dataAlias;
                ValueNode value = tool.getEntry(dataVirtual, index.asJavaConstant().asInt());
                tool.replaceWith(value);
                return;
            }
        }

    }

    @Override
    public Node canonical(CanonicalizerTool tool) {
        return this;
    }
}
