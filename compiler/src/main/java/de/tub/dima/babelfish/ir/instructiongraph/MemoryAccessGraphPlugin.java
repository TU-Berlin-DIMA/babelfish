package de.tub.dima.babelfish.ir.instructiongraph;

import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.memory.BFLoadNode;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.memory.BFStoreNode;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.ResolvedJavaMethod;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.graphbuilderconf.*;

import static org.graalvm.compiler.nodes.NamedLocationIdentity.OFF_HEAP_LOCATION;

public class MemoryAccessGraphPlugin implements GeneratedPluginFactory {

    public void registerPlugins(InvocationPlugins plugins, GeneratedPluginInjectionProvider injection) {
        System.out.println("registered: " + this.getClass().getName());

        plugins.register(new ObjectGetAccess(), UnsafeUtils.class, "getFloat",  long.class);
        plugins.register(new ObjectGetAccess(), UnsafeUtils.class, "getInt",  long.class);
        plugins.register(new ObjectPutAccess(), UnsafeUtils.class, "putInt",  long.class, int.class);
        plugins.register(new ObjectPutAccess(), UnsafeUtils.class, "putLong",  long.class, long.class);
        plugins.register(new ObjectPutAccess(), UnsafeUtils.class, "putChar",  long.class, char.class);
        plugins.register(new ObjectPutAccess(), UnsafeUtils.class, "putFloat",  long.class, float.class);
        plugins.register(new ObjectGetAccess(), UnsafeUtils.class, "getLong",  long.class);
        plugins.register(new ObjectGetAccess(), UnsafeUtils.class, "getShort",  long.class);
        plugins.register(new ObjectGetAccess(), UnsafeUtils.class, "getDouble",  long.class);
        plugins.register(new ObjectGetAccess(), UnsafeUtils.class, "getChar",  long.class);
    }

     private static final class ObjectGetAccess implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod method, Receiver receiver, ValueNode arg) {
            //receiver.get();
            JavaKind kind = method.getSignature().getReturnKind();
            BFLoadNode load = new BFLoadNode(arg, kind, OFF_HEAP_LOCATION);
            b.addPush(kind, load);
            b.getGraph().markUnsafeAccess();
            return true;
        }
    }

    private static final class ObjectPutAccess implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod method, Receiver receiver, ValueNode arg, ValueNode value) {
            //receiver.get();
            JavaKind kind = method.getSignature().getReturnKind();
            BFStoreNode load = new BFStoreNode(arg, value, kind, OFF_HEAP_LOCATION);
            b.add(load);
            b.getGraph().markUnsafeAccess();
            return true;
        }
    }

    private static final class InteropBoundaryEnter implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod method, Receiver receiver, ValueNode arg) {
            //receiver.get();
            JavaKind kind = method.getSignature().getReturnKind();
            BFLoadNode load = new BFLoadNode(arg, kind, OFF_HEAP_LOCATION);
            b.addPush(kind, load);
            b.getGraph().markUnsafeAccess();
            return true;
        }
    }
}
