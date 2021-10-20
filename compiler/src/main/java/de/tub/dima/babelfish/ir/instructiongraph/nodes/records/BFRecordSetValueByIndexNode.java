package de.tub.dima.babelfish.ir.instructiongraph.nodes.records;

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

@NodeInfo(cycles = CYCLES_0, size = SIZE_0, shortName = "LuthObjectSetByIndex")
public class BFRecordSetValueByIndexNode extends FixedWithNextNode implements Virtualizable, Canonicalizable {

    public static final NodeClass<BFRecordSetValueByIndexNode> TYPE = NodeClass.create(BFRecordSetValueByIndexNode.class);

    @Input
    private ValueNode bfRecord;

    @Input
    private ValueNode value;

    private int index;


    public BFRecordSetValueByIndexNode(InvocationPlugin.Receiver luthObjectReceiver, ValueNode index, ValueNode value) {
        super(TYPE, StampFactory.forVoid());
        this.bfRecord = luthObjectReceiver.get();
        this.index = index.asJavaConstant().asInt();
        this.value = value;
    }

    @Override
    public void virtualize(VirtualizerTool tool) {

        if (this.bfRecord instanceof NewBFRecordNode) {
            NewBFRecordNode object = (NewBFRecordNode) bfRecord;
            // Partial Escape finally removed frame
            ValueNode dataAlias = tool.getAlias(object.virtualFrameObjectArray);
            if(dataAlias instanceof VirtualObjectNode){
                VirtualObjectNode dataVirtual = (VirtualObjectNode) dataAlias;

                tool.setVirtualEntry(dataVirtual, index, value, value.getStackKind(), -1);
                tool.delete();
            }
        }

    }

    @Override
    public Node canonical(CanonicalizerTool tool) {
        return this;
    }
}
