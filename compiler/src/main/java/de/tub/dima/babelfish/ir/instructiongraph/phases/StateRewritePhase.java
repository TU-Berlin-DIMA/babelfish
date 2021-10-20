package de.tub.dima.babelfish.ir.instructiongraph.phases;

import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.state.TransactionEndNode;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.state.TransactionStartNode;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.memory.BFLoadNode;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.memory.BFStoreNode;
import jdk.vm.ci.meta.JavaKind;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.iterators.NodeIterable;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.calc.AddNode;
import org.graalvm.compiler.nodes.calc.IntegerEqualsNode;
import org.graalvm.compiler.nodes.cfg.Block;
import org.graalvm.compiler.nodes.cfg.ControlFlowGraph;
import org.graalvm.compiler.nodes.java.AtomicReadAndAddNode;
import org.graalvm.compiler.nodes.java.LoadFieldNode;
import org.graalvm.compiler.nodes.java.LoadIndexedNode;
import org.graalvm.compiler.nodes.java.UnsafeCompareAndSwapNode;
import org.graalvm.compiler.nodes.spi.CoreProviders;
import org.graalvm.compiler.nodes.spi.ValueProxy;
import org.graalvm.compiler.phases.BasePhase;
import org.graalvm.compiler.phases.schedule.SchedulePhase;

import java.util.ArrayList;
import java.util.List;

import static org.graalvm.compiler.nodes.NamedLocationIdentity.OFF_HEAP_LOCATION;

public class StateRewritePhase extends BasePhase<CoreProviders> {

    boolean REPLACE_BY_CAS_LOOP = RuntimeConfiguration.REPLACE_BY_CAS_LOOP;
    boolean REPLACE_BY_ATOMIC = RuntimeConfiguration.REPLACE_BY_ATOMIC;
    boolean REPLACE_WITH_PREAGGREGATION = RuntimeConfiguration.REPLACE_WITH_PREAGGREGATION;;

    @Override
    protected void run(StructuredGraph graph, CoreProviders context) {

        if (graph.getDebug().getDescription().toString().contains("Luth")) {
            NodeIterable<TransactionStartNode> startNodes = graph.getNodes(TransactionStartNode.TYPE);
            NodeIterable<TransactionEndNode> endNodes = graph.getNodes(TransactionEndNode.TYPE);
            if (startNodes.isEmpty() || endNodes.isEmpty()) {
                return;
            }
            ControlFlowGraph cf = ControlFlowGraph.compute(graph, true, true, true, true);

            for (TransactionEndNode endNode : endNodes) {
                TransactionStartNode startNode = findEndNode(endNode);
                if(startNode==null)
                    continue;

                List<BFStoreNode> safeStores = collectStateWriteNodes(startNode, endNode);
                boolean replacedAll = true;
                for (BFStoreNode node : safeStores) {

                    if(node.getValue() instanceof BFLoadNode)
                        continue;

                    boolean replaced = false;
                    if (REPLACE_BY_ATOMIC) {
                        replaced = replaceByAtomic(graph, node, cf);
                    }
                    if (!replaced && REPLACE_BY_CAS_LOOP) {
                        replaced = replaceByCas(graph, node, startNode);
                    }


                    replacedAll = replacedAll && replaced;
                }

                if (replacedAll) {
                    graph.removeFixed(startNode);
                    graph.removeFixed(endNode);
                }
            }
        }
    }

    public TransactionStartNode findEndNode(Node currentNode) {
        while (!(currentNode instanceof TransactionStartNode)) {
            currentNode = currentNode.predecessor();
            if(currentNode==null) {
                //System.out.println("ups");
                return null;
            }
        }
        return (TransactionStartNode) currentNode;
    }

    private UnsafeCompareAndSwapNode createCompareAndSwap(ValueNode address, ValueNode expected, ValueNode newValue) {
        ValueNode object = ConstantNode.forInt(0);
        UnsafeCompareAndSwapNode compAndSwap = new UnsafeCompareAndSwapNode(
                object,
                address,
                expected,
                newValue,
                JavaKind.Int,
                OFF_HEAP_LOCATION
        );
        return compAndSwap;
    }

    public boolean replaceByCas(StructuredGraph graph,
                                BFStoreNode storeNode,
                                TransactionStartNode startNode) {
        // find the maximal control-flow dependency which is still covered by this node.
        // FixedNode root = storeNode;
        // while (isBelow(root.predecessor(), startNode)) {
        //    root = (FixedNode) root.predecessor();
        //}
        System.out.println("Rewrite to CAS");

        // add CAS Loop
        FixedNode loopBody = startNode.next();
        EndNode preLoopEnd = graph.add(new EndNode());
        startNode.setNext(preLoopEnd);
        LoopBeginNode loopBegin = graph.add(new LoopBeginNode());

        /* Add the single non-loop predecessor of the loop header. */
        loopBegin.addForwardEnd(preLoopEnd);

        BFLoadNode loadExpected = graph.addWithoutUniqueWithInputs(new BFLoadNode(
                storeNode.getAddress(),
                JavaKind.Int,
                OFF_HEAP_LOCATION));
        loopBegin.setNext(loadExpected);
        loadExpected.setNext(loopBody);

        FixedNode afterStoreNode = storeNode.next();

        UnsafeCompareAndSwapNode comp = graph.addWithoutUniqueWithInputs(
                createCompareAndSwap(storeNode.getAddress(), loadExpected, storeNode.getValue()));
        graph.replaceFixedWithFixed(storeNode, comp);


        ValueNode one = ConstantNode.forInt(1);
        LogicNode integerCompare = IntegerEqualsNode.create(comp, one, NodeView.DEFAULT);

        LoopExitNode loopExit = graph.add(new LoopExitNode(loopBegin));
        BeginNode ifBeginNode = graph.add(new BeginNode());
        IfNode ifNode = graph.addWithoutUniqueWithInputs(new IfNode(integerCompare, loopExit, ifBeginNode, 0.5));
        comp.setNext(ifNode);
        LoopEndNode loopEndNode = graph.add(new LoopEndNode(loopBegin));
        ifBeginNode.setNext(loopEndNode);
        loopExit.setNext(afterStoreNode);
        return true;
    }

    public boolean replaceByAtomic(StructuredGraph graph, BFStoreNode storeNode, ControlFlowGraph cf) {
        ValueNode value = storeNode.getValue();
        if (value instanceof AddNode) {
            return replaceByAtomicAdd(graph, storeNode, (AddNode) value, cf);
        }
        return false;
    }


    private AtomicReadAndAddNode createAtomicAdd(ValueNode address, ValueNode delta) {
        ValueNode object = ConstantNode.forInt(0);
        AtomicReadAndAddNode addNode = new AtomicReadAndAddNode(
                object,
                address,
                delta,
                delta.getStackKind(),
                OFF_HEAP_LOCATION
        );
        return addNode;
    }

    public boolean replaceByAtomicAdd(StructuredGraph graph,
                                      BFStoreNode storeNode,
                                      AddNode addNode,
                                      ControlFlowGraph cf) {
        BFLoadNode load;
        ValueNode value;

        ValueNode x = addNode.getX();
        if (x instanceof BFLoadNode && ((BFLoadNode) x).getAddress().equals(storeNode.getAddress())) {
            load = (BFLoadNode) addNode.getX();
            value = addNode.getY();
        } else {
            load = (BFLoadNode) addNode.getY();
            value = addNode.getX();
        }




        System.out.println("Rewrite to atomic");
        if (REPLACE_WITH_PREAGGREGATION) {


            ControlFlowGraph cf2 = ControlFlowGraph.compute(graph, true, true, true, true);

            Block block = cf2.blockFor(storeNode);
            if(block.getLoop()==null){
                AtomicReadAndAddNode atomicAdd = graph.addWithoutUniqueWithInputs(
                        createAtomicAdd(load.getAddress(), value));
                atomicAdd.setStateAfter(storeNode.stateAfter());
                graph.replaceFixed(storeNode, atomicAdd);
                return true;
            }
            System.out.println("I'm in a loop");
            LoopBeginNode outerLoopBegin =
                    (LoopBeginNode) block.getLoop().getHeader().getBeginNode();

            //ValuePhiNode phiNode = graph.addWithoutUniqueWithInputs(
            //        new ValuePhiNode(value.stamp(NodeView.DEFAULT),
            //                outerLoopBegin,
            //                new ValueNode[]{ConstantNode.forInt(0)}));

            //System.out.println(phiNode);

            //ValueNode addNodeOfTemp = graph.addWithoutUnique(AddNode.add(phiNode, value));
            //phiNode.values().add(addNodeOfTemp);
          /*  ValueNode originalAddressNode = storeNode.getAddress();
            AddNode coypedAdd = (AddNode) originalAddressNode.copyWithInputs(false);
            coypedAdd = graph.addOrUnique(coypedAdd);
            LoadIndexedNode loadIndexedNodeCopy = (LoadIndexedNode) coypedAdd.getX().copyWithInputs(false);
            loadIndexedNodeCopy = graph.addOrUnique(loadIndexedNodeCopy);
            coypedAdd.setX(loadIndexedNodeCopy);
            graph.addAfterFixed(outerLoopBegin.loopExits().first(), loadIndexedNodeCopy);
            LoadFieldNode loadFieldNodeCopy = (LoadFieldNode) loadIndexedNodeCopy.array().copyWithInputs(false);
            loadFieldNodeCopy = graph.addOrUnique(loadFieldNodeCopy);

            loadIndexedNodeCopy.setArray(loadFieldNodeCopy);
            graph.addAfterFixed(outerLoopBegin.loopExits().first(), loadFieldNodeCopy);
*/

            for(LoopExitNode loopExitNode: outerLoopBegin.loopExits()){

                ValueNode originalAddressNode = storeNode.getAddress();
                AddNode coypedAdd = (AddNode) originalAddressNode.copyWithInputs(false);
                coypedAdd = graph.addOrUnique(coypedAdd);
                LoadIndexedNode loadIndexedNodeCopy = (LoadIndexedNode) coypedAdd.getX().copyWithInputs(false);
                loadIndexedNodeCopy = graph.addOrUnique(loadIndexedNodeCopy);
                coypedAdd.setX(loadIndexedNodeCopy);
                graph.addAfterFixed(loopExitNode, loadIndexedNodeCopy);
                LoadFieldNode loadFieldNodeCopy = (LoadFieldNode) loadIndexedNodeCopy.array().copyWithInputs(false);
                loadFieldNodeCopy = graph.addOrUnique(loadFieldNodeCopy);

                loadIndexedNodeCopy.setArray(loadFieldNodeCopy);
                graph.addAfterFixed(loopExitNode, loadFieldNodeCopy);


                ValueProxy valueProxy = graph.addWithoutUniqueWithInputs(
                        new ValueProxyNode(ConstantNode.forInt(24), loopExitNode)
                );
                ValuePhiNode phiNode = graph.addWithoutUniqueWithInputs(
                                new ValuePhiNode(value.stamp(NodeView.DEFAULT),
                                        outerLoopBegin,
                                       new ValueNode[]{ConstantNode.forInt(0), valueProxy.getOriginalNode()}));

                AtomicReadAndAddNode atomicAdd = graph.addOrUniqueWithInputs(
                        createAtomicAdd(originalAddressNode, phiNode));
                atomicAdd.setStateAfter(loopExitNode.stateAfter());
                graph.addAfterFixed(loopExitNode, atomicAdd);
            }
            //graph.addAfterFixed(loadIndexedNodeCopy, atomicAdd);
            graph.clearLastSchedule();
            graph.createNodeMap();

            //graph.replaceFixed(storeNode, addNode);
            //graph.verify();
            ControlFlowGraph cfg = ControlFlowGraph.compute(graph, true, true, true, true);
            SchedulePhase.run(graph, SchedulePhase.SchedulingStrategy.EARLIEST_WITH_GUARD_ORDER, cfg);

        }else{
            AtomicReadAndAddNode atomicAdd = graph.addWithoutUniqueWithInputs(
                    createAtomicAdd(load.getAddress(), value));
            atomicAdd.setStateAfter(storeNode.stateAfter());
            graph.replaceFixed(storeNode, atomicAdd);
        }
        return true;
    }

    public boolean isBelow(Node currentNode, Node parentNode) {
        while (!currentNode.equals(parentNode)) {
            if (currentNode.predecessor() == null)
                return false;
            currentNode = currentNode.predecessor();
        }
        return true;
    }


    public List<BFStoreNode> collectStateWriteNodes(TransactionStartNode startNode, TransactionEndNode endNode) {
        List<BFStoreNode> safeStores = new ArrayList<>();
        Node currentNode = startNode;
        while (!currentNode.equals(endNode)) {

            if (currentNode instanceof BFStoreNode) {
                safeStores.add((BFStoreNode) currentNode);
            }

            NodeIterable<Node> successors = currentNode.successors();
            if (successors.count() > 1) {
                throw new RuntimeException("ups");
            }
            currentNode = successors.first();
        }

        return safeStores;
    }
}


