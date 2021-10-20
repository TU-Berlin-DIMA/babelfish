package de.tub.dima.babelfish.ir.instructiongraph.phases;

import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.memory.BFLoadNode;
import de.tub.dima.babelfish.ir.instructiongraph.nodes.memory.BFStoreNode;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.nodes.*;
import org.graalvm.compiler.nodes.calc.AndNode;
import org.graalvm.compiler.nodes.calc.BinaryArithmeticNode;
import org.graalvm.compiler.nodes.calc.ConditionalNode;
import org.graalvm.compiler.nodes.extended.UnsafeMemoryStoreNode;
import org.graalvm.compiler.nodes.spi.CoreProviders;
import org.graalvm.compiler.nodes.util.GraphUtil;
import org.graalvm.compiler.phases.BasePhase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SelectionRewritePhase extends BasePhase<CoreProviders> {

    int MAX_ITERATIONS = 1;

    @Override
    protected void run(StructuredGraph graph, CoreProviders context) {

        if (RuntimeConfiguration.ELIMINATE_EMPTY_IF && graph.getDebug().getDescription().toString().contains("BF")) {

            for (int i = 0; i < MAX_ITERATIONS; i++) {
                List<IfNode> ifNodes = graph.getNodes(IfNode.TYPE).snapshot();
                boolean result = processIfs(graph, ifNodes);
                if (!result)
                    break;
                graph.getDebug().dump(1, graph, "Graph_" + i);
            }

        }


    }

    boolean processIfs(StructuredGraph graph, List<IfNode> ifNodes) {
        for (IfNode ifNode : ifNodes) {
            if (RuntimeConfiguration.ELIMINATE_EMPTY_IF_PROFILING) {
                if (ifNode.getTrueSuccessorProbability() < 0.15 || ifNode.getTrueSuccessorProbability() > 0.8) {
                    // dont optimize if.
                    continue;
                }
            }
            if (isQualifyingCondition(ifNode)) {
                boolean eliminateEmptyBlock = eliminateEmptyBlock(graph, ifNode);
                if (!eliminateEmptyBlock) {
                    if (RuntimeConfiguration.ELIMINATE_FILTER_IF && isFilterPrediacte(graph, ifNode)) {
                        eliminateEmptyBlock = eleminateFilterPredicate(graph, ifNode);
                    }
                }
                if (eliminateEmptyBlock)
                    return true;
            }
        }
        return false;
    }

    boolean eleminateFilterPredicate(StructuredGraph graph, IfNode ifNode) {
        System.out.println("Eleminate Filter Predicate ");

        AbstractBeginNode trueCase = ifNode.trueSuccessor();
        AbstractBeginNode falseCase = ifNode.falseSuccessor();
        AbstractBeginNode newNextNode = isEmptyLoopEndBlock(trueCase) ? falseCase : trueCase;
        AbstractBeginNode emptyBlock = isEmptyLoopEndBlock(trueCase) ? trueCase : falseCase;
        List<BFStoreNode> storeNodes = findAllStores(newNextNode);
        for (BFStoreNode storeNode : storeNodes) {
            BFLoadNode loadNode = findLoad(storeNode.getAddress());
            BFLoadNode copyLoad = (BFLoadNode) loadNode.copyWithInputs(false);
            graph.addWithoutUnique(copyLoad);
            graph.addBeforeFixed(storeNode, copyLoad);
            ConditionalNode conditionalNode = graph.addWithoutUnique(new ConditionalNode(ifNode.condition(), copyLoad, storeNode.getValue()));
            UnsafeMemoryStoreNode newStoreNode = new UnsafeMemoryStoreNode(
                    storeNode.getAddress(), conditionalNode, storeNode.getKind(), storeNode.getLocationIdentity());
            newStoreNode = graph.addWithoutUniqueWithInputs(newStoreNode);
            // storeNode.replaceAndDelete(storeNode);
            newStoreNode.setStateAfter(storeNode.stateAfter());
            //storeNode.replaceAndDelete(newStoreNode);
            graph.replaceFixedWithFixed(storeNode, newStoreNode);

            System.out.println("replaces");
        }
        FixedWithNextNode
                oldPre = (FixedWithNextNode) ifNode.predecessor();
        FixedNode oldPost = newNextNode.next();
        //graph.removeSplit(ifNode, newNextNode);

        GraphUtil.unlinkFixedNode(newNextNode);


        ifNode.safeDelete();
        newNextNode.safeDelete();
        oldPre.setNext(oldPost);
        GraphUtil.killCFG(emptyBlock);
        //graph.clearLastSchedule();
        return true;
    }


    BFLoadNode findLoad(ValueNode address) {
        for (Node usage : address.usages().snapshot()) {
            if (usage instanceof BFLoadNode) {
                return (BFLoadNode) usage;
            }
        }
        return null;
    }

    List<BFStoreNode> findAllStores(AbstractBeginNode beginNode) {
        List<BFStoreNode> storeNodes = new ArrayList<>();
        for (FixedNode node : beginNode.getBlockNodes()) {
            if (node instanceof BFStoreNode)
                storeNodes.add((BFStoreNode) node);
        }
        return storeNodes;
    }


    // A filter predicate has a empty edge that gets a loop exit
    boolean isFilterPrediacte(StructuredGraph graph, IfNode ifNode) {
        AbstractBeginNode trueSuccessor = ifNode.trueSuccessor();
        AbstractBeginNode falseSuccessor = ifNode.falseSuccessor();
        if (trueSuccessor.successors().count() != 1 || falseSuccessor.successors().count() != 1)
            return false;
        return isEmptyLoopEndBlock(trueSuccessor) || isEmptyLoopEndBlock(falseSuccessor);
    }


    boolean isEmptyLoopEndBlock(AbstractBeginNode beginNode) {
        return beginNode.successors().first() instanceof LoopEndNode;
    }

    boolean isEmptyBlock(AbstractBeginNode beginNode) {
        return beginNode.successors().first() instanceof AbstractEndNode;
    }


    boolean eliminateEmptyBlock(StructuredGraph graph,
                                IfNode ifNode) {

        MergeNode mergeNode = isEmptyBranch(ifNode);
        if (mergeNode != null && mergeNode.phis().count() == 1) {
            System.out.println("eliminateEmptyBlock node");
            ValuePhiNode phi = (ValuePhiNode) mergeNode.phis().first();
            FixedWithNextNode pre = (FixedWithNextNode) ifNode.predecessor();
            FixedNode afterMergeNode = mergeNode.next();
            GraphUtil.unlinkFixedNode(mergeNode);
            pre.setNext(afterMergeNode);
            AndNode and = graph.addWithoutUnique(new AndNode(phi.valueAt(0), phi.valueAt(1)));

            phi.replaceAtUsagesAndDelete(and);
            //
            mergeNode.stateAfter().safeDelete();
            GraphUtil.killWithUnusedFloatingInputs(ifNode.condition());
            GraphUtil.killCFG(ifNode);

            // graph.removeFixed(mergeNode);


            System.out.println("Branch was empty");
            return true;
        } else {
            return false;
        }

    }

    MergeNode isEmptyBranch(IfNode ifNode) {
        AbstractBeginNode trueSuccessor = ifNode.trueSuccessor();
        AbstractBeginNode falseSuccessor = ifNode.falseSuccessor();
        if (trueSuccessor.successors().count() != 1)
            return null;
        if (falseSuccessor.successors().count() != 1)
            return null;
        if (isEmptyBlock(trueSuccessor) && isEmptyBlock(falseSuccessor)) {
            return (MergeNode) trueSuccessor.successors().first().cfgSuccessors().iterator().next();
        }
        return null;
    }

    boolean isQualifyingCondition(IfNode ifNode) {
        LogicNode condition = ifNode.condition();
        Set<Node> checkedNodes = new HashSet<>();
        return isQualifyingCondition(condition, checkedNodes);
    }

    boolean isQualifyingCondition(ValueNode logicNode, Set<Node> checkedNodes) {
        if (checkedNodes.contains(logicNode))
            return true;
        checkedNodes.add(logicNode);
        if (logicNode instanceof BinaryArithmeticNode) {
            BinaryArithmeticNode intEqual = (BinaryArithmeticNode) logicNode;
            return isQualifyingCondition(intEqual.getX(), checkedNodes) &&
                    isQualifyingCondition(intEqual.getY(), checkedNodes);
        } else if (logicNode instanceof BinaryOpLogicNode) {
            BinaryOpLogicNode intEqual = (BinaryOpLogicNode) logicNode;
            return isQualifyingCondition(intEqual.getX(), checkedNodes) &&
                    isQualifyingCondition(intEqual.getY(), checkedNodes);
        } else if (logicNode instanceof ConditionalNode) {
            ConditionalNode constantNode = (ConditionalNode) logicNode;
            return isQualifyingCondition(constantNode.condition(), checkedNodes);
        } else if (logicNode instanceof ConstantNode) {
            return true;
        } else if (logicNode instanceof BFLoadNode) {
            return true;
        } else if (logicNode instanceof ValuePhiNode) {
            boolean result = true;
            for (ValueNode value : ((ValuePhiNode) logicNode).values()) {
                result = result && isQualifyingCondition(value, checkedNodes);
            }
            return result;
        }
        return false;
    }
}


