package de.tub.dima.babelfish.ir.lqp.udf.java;

import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.record.Record;
import jdk.vm.ci.hotspot.HotSpotObjectConstant;
import jdk.vm.ci.meta.JavaConstant;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.ResolvedJavaField;
import jdk.vm.ci.meta.ResolvedJavaType;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.core.common.type.StampPair;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.extended.UnboxNode;
import org.graalvm.compiler.nodes.graphbuilderconf.*;
import org.graalvm.compiler.nodes.java.LoadFieldNode;
import org.graalvm.compiler.nodes.java.StoreFieldNode;

import java.lang.reflect.Field;

public class JavaTypedUDFGraphPlugins implements GeneratedPluginFactory {


    public void registerPlugins(InvocationPlugins plugins, GeneratedPluginInjectionProvider injection) {
        System.out.println("registered: " + this.getClass().getName());
        plugins.register(new IsPrimitive(), ReflectionHelper.class, "isPrimitive", Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getByteFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getShortFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getIntFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getLongFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getFloatFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getDoubleFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getBooleanFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldValue(), ReflectionHelper.class, "getCharFieldValue", Record.class, Field.class);
        plugins.register(new GetFieldType(), ReflectionHelper.class, "getFieldType", Field.class);
        plugins.register(new SetLuthFieldToTypedRecord(), ReflectionHelper.class, "setFieldValue", Field.class, Record.class, Object.class);
    }

    private final class SetLuthFieldToTypedRecord implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, jdk.vm.ci.meta.ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode fieldValueNode, ValueNode record, ValueNode luthValueNode) {
            if (fieldValueNode.isConstant()) {
                Field field = ((HotSpotObjectConstant) fieldValueNode.asJavaConstant()).asObject(Field.class);
                ResolvedJavaField resolvedJavaField = b.getMetaAccess().lookupJavaField(field);
                if (field.getType().isPrimitive()) {
                    // primitive
                    ValueNode valueNode = UnboxNode.create(b.getMetaAccess(), b.getConstantReflection(), luthValueNode, resolvedJavaField.getType().getJavaKind());
                    b.add(new StoreFieldNode(record, resolvedJavaField, valueNode));
                } else {
                    // assert Arrays.asList(field.getType().getInterfaces()).contains(UDT.class);
                    b.add(new StoreFieldNode(record, resolvedJavaField, luthValueNode));
                }
                return true;
            }
            return false;
        }
    }

    private class IsPrimitive implements InvocationPlugin {
        @Override
        public boolean apply(GraphBuilderContext b, jdk.vm.ci.meta.ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode fieldValueNode) {
            if (fieldValueNode.isConstant()) {
                Field field = ((HotSpotObjectConstant) fieldValueNode.asJavaConstant()).asObject(Field.class);
                if (field.getType().isPrimitive()) {
                    ConstantNode c = ConstantNode.forBoolean(true);
                    b.addPush(c.getStackKind(), c);
                } else {
                    ConstantNode c = ConstantNode.forBoolean(false);
                    b.addPush(c.getStackKind(), c);
                }
                return true;
            }
            return false;
        }
    }


    private class GetFieldValue implements InvocationPlugin {
        @Override
        public boolean apply(GraphBuilderContext b, jdk.vm.ci.meta.ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode record, ValueNode fieldValueNode) {
            if (fieldValueNode.isConstant()) {
                Field field = ((HotSpotObjectConstant) fieldValueNode.asJavaConstant()).asObject(Field.class);
                ResolvedJavaField resolvedJavaField = b.getMetaAccess().lookupJavaField(field);
                if (field.getType().isPrimitive()) {
                    JavaKind resultKind = resolvedJavaField.getJavaKind();
                    StampPair stamp = StampPair.createSingle(StampFactory.forKind(resultKind));
                    LoadFieldNode loadFieldNode = LoadFieldNode.createOverrideStamp(stamp, record, resolvedJavaField);
                    b.addPush(resultKind, loadFieldNode);
                } else {
                    JavaKind resultKind = JavaKind.fromJavaClass(BFType.class);
                    StampPair stamp = StampPair.createSingle(StampFactory.forKind(resultKind));
                    LoadFieldNode loadFieldNode = LoadFieldNode.createOverrideStamp(stamp, record, resolvedJavaField);
                    b.addPush(resultKind, loadFieldNode);
                }
                return true;
            }
            return false;
        }
    }

    private class GetFieldType implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, jdk.vm.ci.meta.ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode fieldValueNode) {
            if (fieldValueNode.isConstant()) {
                Field field = ((HotSpotObjectConstant) fieldValueNode.asJavaConstant()).asObject(Field.class);
                ResolvedJavaField resolvedJavaField = b.getMetaAccess().lookupJavaField(field);
                JavaConstant constant = b.getConstantReflection().asJavaClass((ResolvedJavaType) resolvedJavaField.getType());
                ConstantNode constantNode = ConstantNode.forConstant(StampFactory.forConstant(constant), constant, b.getMetaAccess());
                b.addPush(constant.getJavaKind(), constantNode);
                return true;
            }
            return false;
        }
    }
}
