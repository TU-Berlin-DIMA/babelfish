package de.tub.dima.babelfish.ir.instructiongraph.nodes.records;

import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import jdk.vm.ci.hotspot.*;
import jdk.vm.ci.meta.ConstantReflectionProvider;
import jdk.vm.ci.meta.JavaConstant;
import org.graalvm.compiler.core.common.type.*;
import org.graalvm.compiler.graph.*;
import org.graalvm.compiler.graph.spi.*;
import static org.graalvm.compiler.nodeinfo.NodeCycles.*;
import org.graalvm.compiler.nodeinfo.*;
import static org.graalvm.compiler.nodeinfo.NodeSize.*;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.graphbuilderconf.*;
import org.graalvm.compiler.nodes.spi.*;

@NodeInfo(cycles = CYCLES_0, size = SIZE_0)
public class BFRecordSetValueNode extends BFRecordAccessNode {

    public static final NodeClass<BFRecordSetValueNode> TYPE = NodeClass.create(BFRecordSetValueNode.class);
    @Input
    private ValueNode value;


    public BFRecordSetValueNode(InvocationPlugin.Receiver luthObjectReceiver, ValueNode key, ValueNode value){
        super(TYPE, StampFactory.forVoid(), luthObjectReceiver.get(), key);
        this.value = value;
    }

    @Override
    public void virtualize(VirtualizerTool tool) {
        Node n = tool.getAlias(this.luthObject).usages().first();

        if(!(n instanceof NewBFRecordNode)){
            System.out.println("No NewBFRecordNode" + this.getId());

        }
        NewBFRecordNode luthObjectNode = (NewBFRecordNode) n;

        ConstantReflectionProvider c = tool.getConstantReflection();

        for(int i = 0; i<c.readArrayLength(luthObjectNode.fieldNames);i++){
            JavaConstant arrayElement = c.readArrayElement(luthObjectNode.fieldNames, i);
            if(arrayElement.toValueString().equals(key.asJavaConstant().toValueString())){
                tool.setVirtualEntry(luthObjectNode.virtualFrameObjectArray, i, value);
                tool.delete();
                return;

            }
        }

        System.out.println("Huch here is an error");
    }

    @Override
    public Node canonical(CanonicalizerTool tool) {
        if(luthObject.isConstant()){
            BFRecord object = getLuthObjectValue();
            String keyString = getKey();
            Object partialEvaluatedKey = object.getValue(keyString);
            JavaConstant constentResult = ((HotSpotConstantReflectionProvider) tool.getConstantReflection()).forObject(partialEvaluatedKey);
            return ConstantNode.forConstant(stamp,constentResult, tool.getMetaAccess());

        }
        return this;
    }
}
