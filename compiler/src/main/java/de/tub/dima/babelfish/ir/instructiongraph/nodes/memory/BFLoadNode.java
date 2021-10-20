package de.tub.dima.babelfish.ir.instructiongraph.nodes.memory;

import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.core.common.type.IntegerStamp;
import org.graalvm.compiler.core.common.type.Stamp;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.graph.spi.Canonicalizable;
import org.graalvm.compiler.graph.spi.CanonicalizerTool;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.FixedWithNextNode;
import org.graalvm.compiler.nodes.NodeView;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.memory.OnHeapMemoryAccess;
import org.graalvm.compiler.nodes.memory.ReadNode;
import org.graalvm.compiler.nodes.memory.address.AddressNode;
import org.graalvm.compiler.nodes.memory.address.OffsetAddressNode;
import org.graalvm.compiler.nodes.spi.Lowerable;
import org.graalvm.compiler.nodes.spi.LoweringTool;
import org.graalvm.compiler.nodes.spi.Virtualizable;
import org.graalvm.compiler.nodes.spi.VirtualizerTool;
import org.graalvm.word.LocationIdentity;

import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_2;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_1;

@NodeInfo(cycles = CYCLES_2, size = SIZE_1)
public class BFLoadNode extends FixedWithNextNode implements Lowerable, Canonicalizable, Virtualizable {

    public static final NodeClass<BFLoadNode> TYPE = NodeClass.create(BFLoadNode.class);
    private final String field;

    @Input
    protected ValueNode address;
    protected final JavaKind kind;
    protected final LocationIdentity locationIdentity;

    public BFLoadNode(ValueNode address, JavaKind kind, LocationIdentity locationIdentity) {
        super(TYPE, StampFactory.forKind(kind));
        this.address = address;
        this.kind = kind;
        this.locationIdentity = locationIdentity;
        field="";
    }
    public BFLoadNode(ValueNode address, JavaKind kind, LocationIdentity locationIdentity, String field) {
        super(TYPE, StampFactory.forKind(kind));
        this.address = address;
        this.kind = kind;
        this.locationIdentity = locationIdentity;
        this.field = field;
    }


    public ValueNode getAddress() {
        return address;
    }

    public JavaKind getKind() {
        return kind;
    }

    @Override
    public void lower(LoweringTool tool) {
        StructuredGraph graph = this.graph();
        JavaKind readKind = this.getKind();
        assert readKind != JavaKind.Object;
        Stamp loadStamp = loadStamp(this.stamp(NodeView.DEFAULT), readKind, false);
        AddressNode address = graph.addOrUniqueWithInputs(OffsetAddressNode.create(this.getAddress()));
        ReadNode memoryRead = graph.add(new ReadNode(address, this.getLocationIdentity(), loadStamp, OnHeapMemoryAccess.BarrierType.NONE));
        // An unsafe read must not float otherwise it may float above
        // a test guaranteeing the read is safe.
        memoryRead.setForceFixed(false);
        this.replaceAtUsages(memoryRead);
        graph.replaceFixedWithFixed(this, memoryRead);
    }

    protected Stamp loadStamp(Stamp stamp, JavaKind kind, boolean compressible) {
        switch (kind) {
            case Boolean:
            case Byte:
                return IntegerStamp.OPS.getNarrow().foldStamp(32, 8, stamp);
            case Char:
            case Short:
                return IntegerStamp.OPS.getNarrow().foldStamp(32, 16, stamp);
        }
        return stamp;
    }

    public LocationIdentity getLocationIdentity() {
        return locationIdentity;
    }

    @Override
    public Node canonical(CanonicalizerTool tool) {

        if (tool.allUsagesAvailable()  )
            return this;
        else
            return this;
    }

    @Override
    public void virtualize(VirtualizerTool tool) {
        System.out.println("virtuala");
    }
}
