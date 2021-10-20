package de.tub.dima.babelfish.ir.instructiongraph.nodes.records;

import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import jdk.vm.ci.hotspot.*;
import org.graalvm.compiler.core.common.type.*;
import org.graalvm.compiler.graph.*;
import org.graalvm.compiler.graph.spi.*;
import org.graalvm.compiler.nodeinfo.*;
import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_0;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_0;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.spi.*;

@NodeInfo(cycles = CYCLES_0, size = SIZE_0)
public abstract class BFRecordAccessNode extends FixedWithNextNode implements Virtualizable, Canonicalizable {

    public static final NodeClass<BFRecordAccessNode> TYPE = NodeClass.create(BFRecordAccessNode.class);

    @Input protected ValueNode luthObject;

    @Node.Input
    protected ValueNode key;

    public BFRecordAccessNode(NodeClass<? extends FixedWithNextNode> c, Stamp stamp, ValueNode luthObject, ValueNode key) {
        super(c, stamp);
        this.luthObject = luthObject;
        this.key = key;
    }


    public boolean isLuthObjectConstant(){
        return luthObject.isConstant();
    }

    public BFRecord getLuthObjectValue(){
        return ((HotSpotObjectConstant) this.luthObject.asJavaConstant()).asObject(BFRecord.class);
    }

    public String getKey(){
        return ((HotSpotObjectConstant) (this.key.asJavaConstant())).asObject(String.class);
    }
}
