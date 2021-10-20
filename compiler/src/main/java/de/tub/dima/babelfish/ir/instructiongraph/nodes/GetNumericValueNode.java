package de.tub.dima.babelfish.ir.instructiongraph.nodes;

import org.graalvm.compiler.core.common.type.Stamp;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.graph.NodeClass;
import org.graalvm.compiler.graph.spi.Simplifiable;
import org.graalvm.compiler.graph.spi.SimplifierTool;
import org.graalvm.compiler.nodeinfo.NodeInfo;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.LogicNode;
import org.graalvm.compiler.nodes.NodeView;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.calc.CompareNode;
import org.graalvm.compiler.nodes.calc.FloatingNode;
import org.graalvm.compiler.nodes.calc.IntegerLessThanNode;
import org.graalvm.compiler.nodes.calc.SubNode;

import static org.graalvm.compiler.nodeinfo.NodeCycles.CYCLES_0;
import static org.graalvm.compiler.nodeinfo.NodeSize.SIZE_0;

@NodeInfo(cycles = CYCLES_0, size = SIZE_0)
public class GetNumericValueNode extends FloatingNode implements Simplifiable {

    public static final NodeClass<GetNumericValueNode> TYPE = NodeClass.create(GetNumericValueNode.class);

    @Input
    private ValueNode value;
    @Input
    private ValueNode divisor;

    public GetNumericValueNode(Stamp stamp, ValueNode value, ValueNode divisor) {
        super(TYPE, stamp);
        this.value = value;
        this.divisor = divisor;
    }

    int convertIntValue(ValueNode constantInput) {
        double doubleCompareValue = constantInput.asJavaConstant().asDouble();
        double divisorValue = divisor.asJavaConstant().asDouble();
        return (int) (doubleCompareValue * divisorValue);
    }


    LogicNode getReplacementCompareNode(SimplifierTool tool, CompareNode compareNode) {

        ValueNode constantInput = compareNode.getY().isConstant()
                ? compareNode.getY() : compareNode.getX();

        int intCompareValue = convertIntValue(constantInput);

        ConstantNode constantIntNode = graph().addWithoutUnique(ConstantNode.forInt(intCompareValue));

        NodeView nodeView = NodeView.from(tool);

        LogicNode newCompareNode;
        if(compareNode.getX().equals(this)){
            newCompareNode = CompareNode.createCompareNode(graph(), compareNode.condition(),
                    value,  constantIntNode, tool.getConstantReflection(), nodeView);
        }else{
            newCompareNode = CompareNode.createCompareNode(graph(), compareNode.condition(),
                    constantIntNode, value, tool.getConstantReflection(), nodeView);
        }

       // LogicNode newCompareNode = CompareNode.createCompareNode(graph(), compareNode.condition(),
        //        value, constantIntNode, tool.getConstantReflection(), nodeView);

        tool.addToWorkList(constantIntNode);

        return newCompareNode;
    }

    LogicNode getReplacementSubCompareNode(SimplifierTool tool, SubNode subNode) {

        ValueNode constantInput = subNode.getY().isConstant()
                ? subNode.getY() : subNode.getX();
        int intCompareValue = convertIntValue(constantInput);
        ConstantNode constantIntNode = graph().addWithoutUnique(ConstantNode.forInt(intCompareValue));

        NodeView nodeView = NodeView.from(tool);

        ValueNode newSubNode = graph().addOrUniqueWithInputs(SubNode.create(value, constantIntNode, nodeView));

        CompareNode compareNode = (CompareNode) subNode.usages().first();

        LogicNode newCompareNode;
        if(compareNode.getX().equals(subNode)){
           // newCompareNode = CompareNode.createCompareNode(graph(), compareNode.condition(),
            //        newSubNode,  ConstantNode.forInt(0), tool.getConstantReflection(), nodeView);
            newCompareNode = graph().addOrUniqueWithInputs(
                    IntegerLessThanNode.create(newSubNode, ConstantNode.forInt(0),  nodeView));
        }else{
            newCompareNode = graph().addOrUniqueWithInputs(
                    IntegerLessThanNode.create(ConstantNode.forInt(0), newSubNode, nodeView));
            //newCompareNode = CompareNode.createCompareNode(graph(), compareNode.condition(),
            //        ConstantNode.forInt(0), newSubNode, tool.getConstantReflection(), nodeView);
        }
        compareNode.replaceAndDelete(newCompareNode);
        //subNode.replaceAndDelete(newSubNode);
        //tool.addToWorkList(constantIntNode);
        //tool.addToWorkList(newSubNode);
        tool.addToWorkList(newCompareNode);
        //tool.addToWorkList(this);

        return newCompareNode;
    }


    @Override
    public void simplify(SimplifierTool tool) {
        if (tool.allUsagesAvailable() && this.hasExactlyOneUsage()) {
            System.out.println("Simplify GetNumericValueNode");
            for (Node usage : usages().snapshot()) {
                System.out.println(usage);
                if (usage instanceof CompareNode) {
                    CompareNode compareNode = (CompareNode) usage;
                    // x or y should be constant
                    if (compareNode.getY().isConstant() || compareNode.getX().isConstant()) {
                        LogicNode node = getReplacementCompareNode(tool, compareNode);
                        compareNode.replaceAndDelete(node);
                        tool.addToWorkList(node);
                        //tool.addToWorkList(compareNode);
                        tool.addToWorkList(this);
                    }
                } else if (usage instanceof SubNode) {
                    SubNode subNode = (SubNode) usage;
                    if (subNode.getY().isConstant() ||
                            subNode.getX().isConstant()) {
                        getReplacementSubCompareNode(tool, subNode);
                    }
                }
            }
        }
    }
}
