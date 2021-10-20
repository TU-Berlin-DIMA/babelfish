package de.tub.dima.babelfish.ir.instructiongraph.nodes.records;

import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import jdk.vm.ci.hotspot.*;
import jdk.vm.ci.meta.ConstantReflectionProvider;
import jdk.vm.ci.meta.JavaConstant;
import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.core.common.type.*;
import org.graalvm.compiler.graph.*;
import org.graalvm.compiler.graph.spi.*;
import org.graalvm.compiler.nodeinfo.*;

import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_0;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_0;

import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.graphbuilderconf.*;
import org.graalvm.compiler.nodes.spi.*;
import org.graalvm.compiler.nodes.virtual.*;

@NodeInfo(cycles = CYCLES_0, size = SIZE_0)
public class BFRecordGetValueNode extends BFRecordAccessNode {

    public static final NodeClass<BFRecordGetValueNode> TYPE = NodeClass.create(BFRecordGetValueNode.class);


    public BFRecordGetValueNode(InvocationPlugin.Receiver luthObjectReceiver, ValueNode key, JavaKind kind) {
        super(TYPE, StampFactory.forKind(kind), luthObjectReceiver.get(true), key);
    }

    @Override
    public void virtualize(VirtualizerTool tool) {
        Node parent = ((VirtualInstanceNode) tool.getAlias(this.luthObject)).usages().first();
        if (parent instanceof NewBFRecordNode) {
            NewBFRecordNode luthObjectNode = (NewBFRecordNode) ((VirtualInstanceNode) tool.getAlias(this.luthObject)).usages().first();

            ConstantReflectionProvider c = tool.getConstantReflection();

            for (int i = 0; i < c.readArrayLength(luthObjectNode.fieldNames); i++) {
                JavaConstant arrayElement = c.readArrayElement(luthObjectNode.fieldNames, i);
                if (arrayElement.toValueString().equals(key.asJavaConstant().toValueString())) {
                    ValueNode value = tool.getEntry(luthObjectNode.virtualFrameObjectArray, i);
                    tool.replaceWith(value);
                    return;

                }
            }
        }
    }

    @Override
    public Node canonical(CanonicalizerTool tool) {
        if (luthObject.isConstant()) {
            BFRecord object = getLuthObjectValue();
            String keyString = getKey();
            Object partialEvaluatedKey = object.getValue(keyString);
            JavaConstant constentResult = ((HotSpotConstantReflectionProvider) tool.getConstantReflection()).forObject(partialEvaluatedKey);
            return ConstantNode.forConstant(stamp, constentResult, tool.getMetaAccess());

        }
        return this;
    }
}
