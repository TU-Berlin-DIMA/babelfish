package de.tub.dima.babelfish.ir.instructiongraph.nodes.memory;

import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.graph.spi.Canonicalizable;
import org.graalvm.compiler.graph.spi.CanonicalizerTool;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.AbstractStateSplit;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.memory.OnHeapMemoryAccess;
import org.graalvm.compiler.nodes.memory.WriteNode;
import org.graalvm.compiler.nodes.memory.address.AddressNode;
import org.graalvm.compiler.nodes.memory.address.OffsetAddressNode;
import org.graalvm.compiler.nodes.spi.Lowerable;
import org.graalvm.compiler.nodes.spi.LoweringTool;
import org.graalvm.word.LocationIdentity;

import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_2;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_1;

@NodeInfo(cycles = CYCLES_2, size = SIZE_1)
public class BFStoreNode extends AbstractStateSplit implements Lowerable, Canonicalizable {

    public static final NodeClass<BFStoreNode> TYPE = NodeClass.create(BFStoreNode.class);

    @Input protected ValueNode value;
    @Input protected ValueNode address;
    protected final JavaKind kind;
    protected final LocationIdentity locationIdentity;

    public BFStoreNode(ValueNode address, ValueNode value, JavaKind kind, LocationIdentity locationIdentity) {
        super(TYPE, StampFactory.forVoid());
        this.address = address;
        this.value = value;
        this.kind = kind;
        this.locationIdentity = locationIdentity;
    }


    public ValueNode getAddress() {
        return address;
    }

    public JavaKind getKind() {
        return kind;
    }


    public ValueNode getValue() {
        return value;
    }
    public void setValue(ValueNode value) {
        this.value = value;
    }

    @Override
    public void lower(LoweringTool tool) {
        StructuredGraph graph = this.graph();
        AddressNode address = graph.addOrUniqueWithInputs(OffsetAddressNode.create(getAddress()));
        WriteNode write = graph.add(new WriteNode(address, getLocationIdentity(), value, OnHeapMemoryAccess.BarrierType.NONE));
        write.setStateAfter(stateAfter());
        graph.replaceFixedWithFixed(this, write);
    }


    public LocationIdentity getLocationIdentity() {
        return locationIdentity;
    }

    @Override
    public Node canonical(CanonicalizerTool tool) {

        if (tool.allUsagesAvailable()  ){
            if(value instanceof BFLoadNode){
                if(((BFLoadNode) value).getAddress().equals(getAddress())){
                    return null;
                }
            }
        }
        return this;
    }

}
