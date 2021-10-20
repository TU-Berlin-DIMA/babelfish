package de.tub.dima.babelfish.ir.lqp.udf.java;

import de.tub.dima.babelfish.storage.layout.GenericSerializer;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.udt.UDT;
import de.tub.dima.babelfish.typesytem.valueTypes.Bool;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_16;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_8;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import jdk.vm.ci.hotspot.HotSpotObjectConstant;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.ResolvedJavaField;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.core.common.type.StampPair;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.graphbuilderconf.*;
import org.graalvm.compiler.nodes.java.LoadFieldNode;
import org.graalvm.compiler.nodes.java.NewInstanceNode;
import org.graalvm.compiler.nodes.java.StoreFieldNode;

import java.lang.reflect.Field;
import java.util.Arrays;

public class JavaObjectGraphPlugins implements GeneratedPluginFactory {


    public void registerPlugins(InvocationPlugins plugins, GeneratedPluginInjectionProvider injection) {
        System.out.println("registered: " + this.getClass().getName());

        plugins.register(new ObjectSetField(), GenericSerializer.class, "setFieldToObject", Object.class, Field.class, BFType.class);
        plugins.register(new ObjectGetField(), GenericSerializer.class, "getValueFromObject", Object.class, Field.class);
    }

    private static final class ObjectGetField implements InvocationPlugin {


        @Override
        public boolean apply(GraphBuilderContext b, jdk.vm.ci.meta.ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode record, ValueNode fieldValueNode) {
            if (fieldValueNode.isConstant()) {
                Field field = ((HotSpotObjectConstant) fieldValueNode.asJavaConstant()).asObject(Field.class);
                if (field.getType().isPrimitive()) {
                    try {
                        return getPrimitive(b, field, record);
                    } catch (NoSuchFieldException e) {
                        e.printStackTrace();
                    }
                } else {
                    ResolvedJavaField resolvedJavaField = b.getMetaAccess().lookupJavaField(field);
                    JavaKind resultKind = JavaKind.fromJavaClass(BFType.class);
                    StampPair stamp = StampPair.createSingle(StampFactory.forKind(resultKind));
                    LoadFieldNode loadFieldNode = LoadFieldNode.createOverrideStamp(stamp, record, resolvedJavaField);
                    b.addPush(JavaKind.fromJavaClass(BFType.class), loadFieldNode);
                    return true;
                }
            }
            return false;
        }

        private boolean getPrimitive(GraphBuilderContext b, Field field, ValueNode record) throws NoSuchFieldException {
            LoadFieldNode loadFieldNode = LoadFieldNode.create(b.getAssumptions(), record, b.getMetaAccess().lookupJavaField(field));
            b.add(loadFieldNode);
            NewInstanceNode newInstanceNode = null;
            Field valueField = null;
            Class<?> type = field.getType();
            if (type == byte.class) {
                newInstanceNode = new NewInstanceNode(b.getMetaAccess().lookupJavaType(Int_8.class), false);
                valueField = Int_8.class.getDeclaredField("value");
            } else if (type == short.class) {
                newInstanceNode = new NewInstanceNode(b.getMetaAccess().lookupJavaType(Int_16.class), false);
                valueField = Int_16.class.getDeclaredField("value");
            } else if (type == int.class) {
                newInstanceNode = new NewInstanceNode(b.getMetaAccess().lookupJavaType(Int_32.class), false);
                valueField = Int_32.class.getDeclaredField("value");
            } else if (type == long.class) {
                newInstanceNode = new NewInstanceNode(b.getMetaAccess().lookupJavaType(Int_64.class), false);
                valueField = Int_64.class.getDeclaredField("value");
            } else if (type == float.class) {
                newInstanceNode = new NewInstanceNode(b.getMetaAccess().lookupJavaType(Float_32.class), false);
                valueField = Float_32.class.getDeclaredField("value");
            } else if (type == double.class) {
                newInstanceNode = new NewInstanceNode(b.getMetaAccess().lookupJavaType(Float_64.class), false);
                valueField = Float_64.class.getDeclaredField("value");
            } else if (type == boolean.class) {
                newInstanceNode = new NewInstanceNode(b.getMetaAccess().lookupJavaType(Bool.class), false);
                valueField = Bool.class.getDeclaredField("value");
            } else if (type == char.class) {
                newInstanceNode = new NewInstanceNode(b.getMetaAccess().lookupJavaType(Char.class), false);
                valueField = Char.class.getDeclaredField("value");
            }

            newInstanceNode = b.addPush(JavaKind.fromJavaClass(BFType.class), newInstanceNode);
            ResolvedJavaField resolvedField = b.getMetaAccess().lookupJavaField(valueField);
            b.add(new StoreFieldNode(newInstanceNode, resolvedField, loadFieldNode));

            return true;
        }
    }

    private static final class ObjectSetField implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, jdk.vm.ci.meta.ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode record, ValueNode schema, ValueNode value) {
            if (schema.isConstant()) {
                Field field = ((HotSpotObjectConstant) schema.asJavaConstant()).asObject(Field.class);
                ResolvedJavaField resolvedJavaField = b.getMetaAccess().lookupJavaField(field);
                try {
                    value = getValue(b, resolvedJavaField, value);
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }
                b.add(new StoreFieldNode(record, resolvedJavaField, value));
                return true;
            }
            return false;
        }

        private ValueNode getValue(GraphBuilderContext b, ResolvedJavaField field, ValueNode value) throws NoSuchFieldException {
            Field asByteField = Int_8.class.getDeclaredField("value");
            Field asShortField = Int_16.class.getDeclaredField("value");
            Field asIntField = Int_32.class.getDeclaredField("value");
            Field asLongField = Int_64.class.getDeclaredField("value");
            Field asFloatField = Float_32.class.getDeclaredField("value");
            Field asDoubleField = Float_64.class.getDeclaredField("value");
            Field asBooleanField = Bool.class.getDeclaredField("value");
            JavaKind fieldKind = field.getType().getJavaKind();

            if (fieldKind.isObject()) {
                try {
                    Class<?> clazz = Class.forName(field.getType().toClassName());
                    if (Arrays.asList(clazz.getInterfaces()).contains(UDT.class)) {
                        return value;
                    }

                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            if (fieldKind.toJavaClass().isAssignableFrom(BFType.class))
                return value;
            if (fieldKind.isPrimitive()) {

                switch (fieldKind) {
                    case Byte:
                        return b.add(LoadFieldNode.create(b.getAssumptions(), value, b.getMetaAccess().lookupJavaField(asByteField)));
                    case Short:
                        return b.add(LoadFieldNode.create(b.getAssumptions(), value, b.getMetaAccess().lookupJavaField(asShortField)));
                    case Int:
                        return b.add(LoadFieldNode.create(b.getAssumptions(), value, b.getMetaAccess().lookupJavaField(asIntField)));
                    case Long:
                        return b.add(LoadFieldNode.create(b.getAssumptions(), value, b.getMetaAccess().lookupJavaField(asLongField)));
                    case Float:
                        return b.add(LoadFieldNode.create(b.getAssumptions(), value, b.getMetaAccess().lookupJavaField(asFloatField)));
                    case Double:
                        return b.add(LoadFieldNode.create(b.getAssumptions(), value, b.getMetaAccess().lookupJavaField(asDoubleField)));
                    case Boolean:
                        return b.add(LoadFieldNode.create(b.getAssumptions(), value, b.getMetaAccess().lookupJavaField(asBooleanField)));
                }
            }
            return null;
        }
    }
}
