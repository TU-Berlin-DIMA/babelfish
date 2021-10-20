package de.tub.dima.babelfish.ir.instructiongraph.nodes.records;


import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import jdk.vm.ci.hotspot.*;
import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.core.common.type.*;
import org.graalvm.compiler.graph.*;
import org.graalvm.compiler.graph.spi.*;
import org.graalvm.compiler.nodeinfo.*;
import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_0;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_0;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.spi.*;

@NodeInfo(cycles = CYCLES_0, size = SIZE_0)
public class BFRecordGetFieldIndex extends FixedWithNextNode implements Virtualizable, Canonicalizable {

    public static final NodeClass<BFRecordGetFieldIndex> TYPE = NodeClass.create(BFRecordGetFieldIndex.class);
    @Input
    private ValueNode schemaObject;
    @Input
    private ValueNode key;

    public BFRecordGetFieldIndex(ValueNode schemaObject, ValueNode key) {
        super(TYPE, StampFactory.forKind(JavaKind.Int));
        this.schemaObject = schemaObject;
        this.key = key;
    }

    @Override
    public Node canonical(CanonicalizerTool tool) {

        if(schemaObject.isConstant()){
            RecordSchema object = ((HotSpotObjectConstant) this.schemaObject.asJavaConstant()).asObject(RecordSchema.class);
            String key = getKey();
            int index = object.getFieldIndex(key);
            return ConstantNode.forInt(index);
        }
        return this;
    }

    public String getKey(){
        return ((HotSpotObjectConstant) (this.key.asJavaConstant())).asObject(String.class);
    }

    @Override
    public void virtualize(VirtualizerTool tool) {

    }
}
