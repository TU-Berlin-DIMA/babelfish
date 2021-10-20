package de.tub.dima.babelfish.ir.instructiongraph;

import com.oracle.truffle.api.Truffle;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.records.*;
import jdk.vm.ci.code.Architecture;
import jdk.vm.ci.hotspot.HotSpotConstantReflectionProvider;
import jdk.vm.ci.hotspot.HotSpotObjectConstant;
import jdk.vm.ci.meta.*;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.graphbuilderconf.*;
import org.graalvm.compiler.phases.util.Providers;
import org.graalvm.compiler.serviceprovider.ServiceProvider;
import org.graalvm.compiler.truffle.compiler.substitutions.TruffleInvocationPluginProvider;
import org.graalvm.compiler.truffle.runtime.hotspot.AbstractHotSpotTruffleRuntime;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@ServiceProvider(TruffleInvocationPluginProvider.class)
public class LuthObjectTruffleGraphPlugins implements TruffleInvocationPluginProvider {

    @Override
    public void registerInvocationPlugins(Providers providers, Architecture architecture, InvocationPlugins plugins, boolean canDelayIntrinsification) {
        System.out.println("truffle registered: " + this.getClass().getName());

        AbstractHotSpotTruffleRuntime test = ((AbstractHotSpotTruffleRuntime) Truffle.getRuntime());

        /*ResolvedJavaType luthObjectType = getRuntime().resolveType(providers.getMetaAccess(), "de.tub.dima.luth.plan.compiler.object.BFRecord");
        ResolvedJavaType intType = getRuntime().resolveType(providers.getMetaAccess(), int.class.getName());
        InvocationPlugins.Registration r = new InvocationPlugins.Registration(plugins, new InvocationPlugins.ResolvedJavaSymbol(luthObjectType));
        InvocationPlugins.ResolvedJavaSymbol frameSlotType = new InvocationPlugins.ResolvedJavaSymbol(providers.g);

        r.register2("getByIndex", InvocationPlugin.Receiver.class, frameSlotType, new InvocationPlugin() {
            @Override
            public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver frameNode, ValueNode frameSlotNode) {
                int frameSlotIndex = maybeGetConstantFrameSlotIndex(frameNode, frameSlotNode, constantReflection, types);
                if (frameSlotIndex >= 0) {
                    b.addPush(accessKind, new VirtualFrameGetNode(frameNode, frameSlotIndex, accessKind, accessTag));
                    return true;
                }
                return false;
            }
        });

        plugins.register(new InvocationPlugin() {
            @Override
            public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode schema) {
                if (canDelayIntrinsification) {
                    return false;
                }
                if (!schema.isJavaConstant()) {
                    throw b.bailout("Parameter 'descriptor' is not a compile-time constant");
                }
                b.addPush(JavaKind.Object, new NewBFRecordNode(b, schema));
                return true;
            }
        }, BFRecord.class, "createObject", RecordSchema.class);



        plugins.register(new InvocationPlugin() {
            @Override
            public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode index, ValueNode value) {
                if (index.isConstant()) {
                    b.add(new BFRecordSetValueByIndexNode(receiver, index, value));
                    return true;
                }
                return false;
            }
        }, BFRecord.class, "setValue", InvocationPlugin.Receiver.class, int.class, BFType.class);
*/

        // HotSpotTruffleCompiler compiler = test.getTruffleCompiler();
        //plugins.register(new ObjectGetAccess(), BFRecord.class, "getValue", InvocationPlugin.Receiver.class, String.class);

        //plugins.register(new ObjectSetAccess(), BFRecord.class, "setValue", InvocationPlugin.Receiver.class, String.class, BFType.class);
        //plugins.register(new ObjectFindIndex(), RecordSchema.class, "getFieldIndex", InvocationPlugin.Receiver.class, String.class);
        //plugins.register(new ObjectFindIndex(), RecordSchema.class, "getFieldIndexFromConstant", InvocationPlugin.Receiver.class, String.class);
        //plugins.register(new CreateLuthObject(), BFRecord.class, "createObject", RecordSchema.class);
        //plugins.register(new InlineMethod(), Date.class, "parse", String.class);
        //plugins.register(new DynmicObjectGetShape(), DynamicObjectImpl.class, "getShape", InvocationPlugin.Receiver.class);
        // plugins.register(new SerializeSubstitution(), UDFRecordSerializer.class, "serializeRecordToFrame", Object.class, Frame.class);
/*
        plugins.register(new InvocationPlugin() {
            @Override
            public boolean apply(GraphBuilderContext b, ResolvedJavaMethod targetMethod, Receiver receiver, ValueNode array, ValueNode index, ValueNode clazz) {
                if (index.isConstant() && clazz.isConstant()) {
                    JavaKind kind = JavaKind.Object;
                    ConstantReflectionProvider constantReflection = b.getConstantReflection();
                    ResolvedJavaType javaType = constantReflection.asJavaType(clazz.asConstant());
                    TypeReference t =  TypeReference.createExactTrusted(javaType);
                    ObjectStamp stamp = StampFactory.object(t, true);
                    b.addPush(kind, new LuthObjectLoadIndexNode(stamp, array, index, kind));
                    return true;
                }
                return false;
            }
        }, BFRecord.class, "getValueWithClass", InvocationPlugin.Receiver.class, Object[].class, int.class, Class.class);

*/
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
