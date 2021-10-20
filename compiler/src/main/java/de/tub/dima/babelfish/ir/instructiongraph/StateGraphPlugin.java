package de.tub.dima.babelfish.ir.instructiongraph;

import de.tub.dima.babelfish.ir.instructiongraph.nodes.state.TransactionEndNode;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.state.TransactionStartNode;
import de.tub.dima.babelfish.ir.pqp.nodes.state.BFTransactionNode;
import jdk.vm.ci.meta.JavaKind;
import jdk.vm.ci.meta.ResolvedJavaMethod;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.graphbuilderconf.*;

public class StateGraphPlugin implements GeneratedPluginFactory {

    public void registerPlugins(InvocationPlugins plugins, GeneratedPluginInjectionProvider injection) {
        System.out.println("registered: " + this.getClass().getName());
        plugins.register(new TransactionStart(), BFTransactionNode.StartTransactionNode.class, "getLock",  long.class);
        plugins.register(new TransactionEnd(), BFTransactionNode.class, "unlock",  long.class);
    }

    private static final class TransactionStart implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod method, Receiver receiver, ValueNode arg) {
            //receiver.get();
            JavaKind kind = method.getSignature().getReturnKind();
            TransactionStartNode load = new TransactionStartNode(arg);
            b.addPush(kind, load);
            return true;
        }
    }
    private static final class TransactionEnd implements InvocationPlugin {

        @Override
        public boolean apply(GraphBuilderContext b, ResolvedJavaMethod method, Receiver receiver, ValueNode arg) {
            //receiver.get();
            JavaKind kind = method.getSignature().getReturnKind();
            TransactionEndNode load = new TransactionEndNode(arg);
            b.addPush(kind, load);
            return true;
        }
    }
}
