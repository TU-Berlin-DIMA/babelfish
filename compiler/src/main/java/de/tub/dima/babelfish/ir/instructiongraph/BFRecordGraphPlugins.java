package de.tub.dima.babelfish.ir.instructiongraph;

import com.oracle.truffle.api.Truffle;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.GetNumericValueNode;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.records.*;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import jdk.vm.ci.hotspot.HotSpotConstantReflectionProvider;
import jdk.vm.ci.hotspot.HotSpotObjectConstant;
import jdk.vm.ci.meta.JavaConstant;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.ResolvedJavaMethod;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.graphbuilderconf.*;
import org.graalvm.compiler.truffle.runtime.hotspot.AbstractHotSpotTruffleRuntime;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class BFRecordGraphPlugins implements GeneratedPluginFactory {


    public void registerPlugins(InvocationPlugins plugins, GeneratedPluginInjectionProvider injection) {
        System.out.println("registered: " + this.getClass().getName());

        AbstractHotSpotTruffleRuntime test = ((AbstractHotSpotTruffleRuntime) Truffle.getRuntime());
        // HotSpotTruffleCompiler compiler = test.getTruffleCompiler();
        //plugins.register(new ObjectGetAccess(), BFRecord.class, "getValue", InvocationPlugin.Receiver.class, String.class);
        //plugins.register(new ObjectGetByIndexAccess(), BFRecord.class, "getByIndex", InvocationPlugin.Receiver.class, int.class);
        //plugins.register(new ObjectSetAccess(), BFRecord.class, "setValue", InvocationPlugin.Receiver.class, String.class, BFType.class);
        //plugins.register(new ObjectFindIndex(), RecordSchema.class, "getFieldIndex", InvocationPlugin.Receiver.class, String.class);
        //plugins.register(new ObjectFindIndex(), RecordSchema.class, "getFieldIndexFromConstant", InvocationPlugin.Receiver.class, String.class);
        //plugins.register(new CreateLuthObject(), BFRecord.class, "createObject", RecordSchema.class);
        //plugins.register(new InlineMethod(), Date.class, "parse", String.class);
        //plugins.register(new DynmicObjectGetShape(), DynamicObjectImpl.class, "getShape", InvocationPlugin.Receiver.class);
        // plugins.register(new SerializeSubstitution(), UDFRecordSerializer.class, "serializeRecordToFrame", Object.class, Frame.class);

        //plugins.register(new ObjectGetAccess(), BFRecord.class, "getValue", InvocationPlugin.Receiver.class, String.class);
        //plugins.register(new ObjectSetAccess(), BFRecord.class, "setValue", InvocationPlugin.Receiver.class, String.class, BFType.class);
       // plugins.register(new ObjectFindIndex(), RecordSchema.class, "getFieldIndex", InvocationPlugin.Receiver.class, String.class);
        plugins.register(new ObjectFindIndex(), RecordSchema.class, "getFieldIndexFromConstant", InvocationPlugin.Receiver.class, String.class);
        //plugins.register(new CreateLuthObject(), BFRecord.class, "createObject", RecordSchema.class);
        plugins.register(new InlineMethod(), Date.class, "parse", String.class);
        plugins.register(new GetNumericValue(JavaKind.Float), Numeric.class, "getFloatValue", long.class, float.class);
        plugins.register(new GetNumericValue(JavaKind.Double), Numeric.class, "getDoubleValue", long.class, double.class);
    }

    private static final class GetNumericValue implements InvocationPlugin {

        private final JavaKind kind;

        private GetNumericValue(JavaKind kind) {
            this.kind = kind;
        }

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver,  ValueNode value, ValueNode divisor) {
            if (!divisor.isConstant())
                return false;
            b.addPush(kind, new GetNumericValueNode(StampFactory.forKind(kind), value, divisor));
            return true;
        }
    }

    private static final class ObjectGetAccess implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode arg) {
            JavaKind kind = JavaKind.Object;
            b.addPush(kind, new BFRecordGetValueNode(receiver, arg, kind));
            return true;
        }
    }

    private static final class ObjectGetByIndexAccess implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode arg) {
            JavaKind kind = JavaKind.Object;
            b.addPush(kind, new BFRecordGetValueByIndexNode(receiver, arg, kind));
            return true;
        }


    }

    private static final class InlineMethod implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod method, Receiver receiver, ValueNode string) {
            if (!string.isConstant())
                return false;
            String name = method.getName();
            String className = method.getDeclaringClass().toClassName();
            Method[] methods = new Method[0];
            try {
                methods = Class.forName(className).getDeclaredMethods();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            for (Method m : methods) {
                if (m.getName().equals(name)) {
                    try {
                        m.setAccessible(true);
                        Object[] objectArgs = new Object[m.getParameterCount()];
                        for (int i = 0; i < m.getParameterCount(); i++) {
                            ResolvedJavaMethod.Parameter p = method.getParameters()[i];
                            objectArgs[i] = ((HotSpotObjectConstant) string.asJavaConstant()).asObject(b.getMetaAccess().lookupJavaType(string.asJavaConstant()));
                        }
                        Object result = m.invoke(null, objectArgs);
                        JavaConstant constObject = ((HotSpotConstantReflectionProvider) b.getConstantReflection()).forObject(result);
                        b.addPush(method.getSignature().getReturnKind(), ConstantNode.forConstant(constObject, b.getMetaAccess()));
                        return true;
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }
            return true;
        }
    }

    private static final class ObjectSetAccess implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode key, ValueNode valueNode) {
            b.append(new BFRecordSetValueNode(receiver, key, valueNode));
            return true;
        }


    }

    private static final class CreateLuthObject implements InvocationPlugin {
        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode arg) {
            //ValueNode nonNullArguments = b.add(PiNode.create(arg, StampFactory.objectNonNull(StampTool.typeReferenceOrNull(arg))));
            if (arg.isConstant()) {

                b.addPush(JavaKind.Object, new NewBFRecordNode(b, arg));
                return true;
            }
            return false;

        }
    }

    private static final class ObjectFindIndex implements InvocationPlugin {
        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode arg) {
            JavaKind kind = JavaKind.Int;
            b.addPush(kind, new BFRecordGetFieldIndex(receiver.get(), arg));
            return true;
        }
    }

    private class DynmicObjectGetShape implements InvocationPlugin {
        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode arg) {
            return false;
        }
    }
}
