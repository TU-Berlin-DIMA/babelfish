package de.tub.dima.babelfish.ir.instructiongraph.nodes.records;


import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import jdk.vm.ci.meta.ConstantReflectionProvider;
import jdk.vm.ci.meta.JavaConstant;
import jdk.vm.ci.meta.MetaAccessProvider;
import jdk.vm.ci.meta.ResolvedJavaField;
import jdk.vm.ci.meta.ResolvedJavaType;
import org.graalvm.compiler.core.common.type.*;
import org.graalvm.compiler.graph.*;
import org.graalvm.compiler.graph.spi.*;
import org.graalvm.compiler.nodeinfo.*;
import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_0;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_0;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.graphbuilderconf.*;
import org.graalvm.compiler.nodes.java.*;
import org.graalvm.compiler.nodes.spi.*;
import org.graalvm.compiler.nodes.virtual.*;

import java.util.*;

@NodeInfo(cycles = CYCLES_0, size = SIZE_0)
public class NewBFRecordNode extends NewInstanceNode implements VirtualizableAllocation, Canonicalizable {

    public static final NodeClass<NewBFRecordNode> TYPE = NodeClass.create(NewBFRecordNode.class);
    final JavaConstant fieldNames;

    @Input
    private ConstantNode defaultValue;

    @Input
    private ValueNode objectSchemaNode;

    @Input
    private VirtualInstanceNode virtualInstance;

    @Input
    public VirtualObjectNode virtualFrameObjectArray;

    public NewBFRecordNode(GraphBuilderContext b, ValueNode arg) {
       // super(TYPE, StampFactory.objectNonNull(TypeReference.createExactTrusted(b.getMetaAccess().lookupJavaType(BFRecord.class))));
        super(TYPE, StampFactory.objectNonNull(TypeReference.createExactTrusted(b.getMetaAccess().lookupJavaType(BFRecord.class))).type(), false, null);

        ConstantReflectionProvider constantReflection = b.getConstantReflection();
        MetaAccessProvider metaAccess = b.getMetaAccess();

        ResolvedJavaType luthObjectType = metaAccess.lookupJavaType(BFRecord.class);
        ResolvedJavaType objectSchemaType = metaAccess.lookupJavaType(RecordSchema.class);
        ResolvedJavaField[] rf = objectSchemaType.getInstanceFields(true);

        this.objectSchemaNode = arg;
        jdk.vm.ci.meta.JavaConstant objectSchema = arg.asJavaConstant();
        this.fieldNames = constantReflection.readFieldValue(findField(rf, "fieldNames"), objectSchema);




        StructuredGraph graph = b.getGraph();

        this.defaultValue = ConstantNode.forConstant(JavaConstant.NULL_POINTER,metaAccess, graph);


        ResolvedJavaField[] resolvedFields = luthObjectType.getInstanceFields(true);
        this.virtualInstance = graph.add(new VirtualInstanceNode(luthObjectType, resolvedFields, true));
        ResolvedJavaField localsField = findField(resolvedFields, "values");
        this.virtualFrameObjectArray = graph.add(new VirtualArrayNode((ResolvedJavaType) localsField.getType().getComponentType(), 32));
    }

    @Override
    public Node canonical(CanonicalizerTool tool) {
        if (tool.allUsagesAvailable() && hasNoUsages()) {
            return null;
        }
        return this;
    }

    @Override
    public void virtualize(VirtualizerTool tool) {
        System.out.println("Virtualze LuthObjectNode + " + this.getId());

        ValueNode[] objectArrayEntryState = new ValueNode[32];
        Arrays.fill(objectArrayEntryState, defaultValue);

        tool.createVirtualObject(virtualFrameObjectArray, objectArrayEntryState, Collections.<MonitorIdNode> emptyList(), false);

        ValueNode[] frameEntryState = new ValueNode[2];
        frameEntryState[0] = virtualFrameObjectArray;
        frameEntryState[1] = objectSchemaNode;

       tool.createVirtualObject(virtualInstance, frameEntryState, Collections.<MonitorIdNode> emptyList(), true);
       tool.replaceWithValue(tool.getAlias(virtualInstance));
       // tool.replaceWithVirtual(virtualInstance);

    }

    private static ResolvedJavaField findField(ResolvedJavaField[] fields, String fieldName) {
        for (ResolvedJavaField field : fields) {
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }
}
