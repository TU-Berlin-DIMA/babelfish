package de.tub.dima.babelfish.ir.pqp;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.nodes.ExecutableNode;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.*;
import de.tub.dima.babelfish.ir.lqp.relational.*;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.schema.FieldStamp;
import de.tub.dima.babelfish.ir.lqp.udf.*;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaTypedUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonPandasUDF;
import de.tub.dima.babelfish.ir.pqp.nodes.*;
import de.tub.dima.babelfish.ir.pqp.nodes.polyglot.*;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.groupby.*;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.join.BFJoinBuildOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.join.BFJoinProbOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.join.JoinContext;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.map.BFMapOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.map.ExpressionNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.map.ExpressionNodeFactory;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.project.BFProjectionOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.BFParallelScanOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.BFScanOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.arrow.BFArrowScanOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.csv.BFCSVScanOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.selection.BFSelectionOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.selection.PredicateNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.selection.PredicateNodeFactory;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks.BFMemorySink;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks.BFPrintSink;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.storage.layout.fields.PhysicalFieldFactory;

import java.util.ArrayList;

/**
 * The PQP generator receives a BabelfishEngine LQP and generated the corresponding phyiscal query plan.
 * This step involves pipelining, whereby BabelfishEngine
 * currently considers aggregation and joins as the only pipelines-breakers.
 * This step involves a recursive traversal over the LQP.
 */
public class PQPGenerator {

    BabelfishEngine.BabelfishContext babelfishContext;

    public PQPGenerator(BabelfishEngine.BabelfishContext babelfishContext) {
        this.babelfishContext = babelfishContext;
    }

    public BFQueryRootNode generate(LogicalQueryPlan planObject) {
        // create global frame descriptor
        FrameDescriptor frameDescriptor = new FrameDescriptor();
        BabelfishEngine lang = babelfishContext.getLuthLanguage();

        ArrayList<BFPipelineRoot> pipelines = new ArrayList<>();
        processOperator(null, lang, planObject.getSource(), frameDescriptor, pipelines);

        BFQueryRootNode queryNode = new BFQueryRootNode(lang, frameDescriptor, pipelines.toArray(new BFPipelineRoot[0]));
        return queryNode;
    }

    private BFOperator processOperator(BFOperator next,
                                       BabelfishEngine lang,
                                       LogicalOperator node,
                                       FrameDescriptor frameDescriptor,
                                       ArrayList<BFPipelineRoot> pipelines) {
        return createPhysicalOperator(next, lang, node, frameDescriptor, pipelines);
    }

    private BFOperator createPhysicalOperator(BFOperator next,
                                              BabelfishEngine lang,
                                              LogicalOperator node,
                                              FrameDescriptor frameDescriptor,
                                              ArrayList<BFPipelineRoot> pipelines) {
        if (node instanceof Scan) {
            BFOperatorInterface scan = createScan(lang, (Scan) node, next, frameDescriptor);
            BFPipelineRoot pipeline = new BFPipelineRoot(scan);
            pipelines.add(0, pipeline);
            return null;
        } else if (node instanceof Sink) {
            BFOperator sink = createSink(lang, (Sink) node, frameDescriptor);
            return createPhysicalOperator(sink, lang, node.getParents().get(0), frameDescriptor, pipelines);
        } else if (node instanceof GroupBy) {
            BFOperator groupBy = createGroupBy(lang, (GroupBy) node, frameDescriptor, next, pipelines);
            return createPhysicalOperator(groupBy, lang, node.getParents().get(0), frameDescriptor, pipelines);
        } else if (node instanceof Selection) {
            BFOperator selection = createSelection(lang, (Selection) node, frameDescriptor, next);
            return createPhysicalOperator(selection, lang, node.getParents().get(0), frameDescriptor, pipelines);
        } else if (node instanceof Function) {
            BFOperator map = createMap(lang, (Function) node, frameDescriptor, next);
            return createPhysicalOperator(map, lang, node.getParents().get(0), frameDescriptor, pipelines);
        } else if (node instanceof Projection) {
            BFOperator projection = createProjection(lang, (Projection) node, frameDescriptor, next);
            return createPhysicalOperator(projection, lang, node.getParents().get(0), frameDescriptor, pipelines);
        } else if (node instanceof PolyglotUDFOperator) {
            BFOperatorInterface poly = processPolyglotUDF(lang, (PolyglotUDFOperator) node, frameDescriptor, next);
            if(poly instanceof BFScalarUDFOperatorForPythonPandas){
                // handle special case if poly operator is a scan
                BFPipelineRoot pipeline = new BFPipelineRoot(poly);
                pipelines.add(0, pipeline);
                return null;
            }
            return createPhysicalOperator((BFOperator) poly, lang, node.getParents().get(0), frameDescriptor, pipelines);
        } else if (node instanceof Join) {
            createJoin(lang, (Join) node, frameDescriptor, next, pipelines);
            return null;
        } else if (node instanceof UDFOperator) {
            BFOperator udf = processUDF(lang, (UDFOperator) node, frameDescriptor, next);
            return createPhysicalOperator(udf, lang, node.getParents().get(0), frameDescriptor, pipelines);
        } else if (node instanceof CSVScan) {
            BFOperatorInterface scan = createCSVScan(lang, (CSVScan) node, next, frameDescriptor);
            BFPipelineRoot pipeline = new BFPipelineRoot(scan);
            pipelines.add(0, pipeline);
            return null;
        } else if (node instanceof ArrowScan) {
            BFOperatorInterface scan = createArrowScan(lang, (ArrowScan) node, next, frameDescriptor);
            BFPipelineRoot pipeline = new BFPipelineRoot(scan);
            pipelines.add(0, pipeline);
            return null;
        } else if (node instanceof ParallelScan) {
            BFOperatorInterface scan = createParallelScan(lang, (ParallelScan) node, next, frameDescriptor);
            BFPipelineRoot pipeline = new BFPipelineRoot(scan);
            pipelines.add(0, pipeline);
            return null;
        }
        /*
    else if (node instanceof CSVScan) {
            LuthCallableOperator scan = createCSVScan(lang, (CSVScan) node, next, frameDescriptor);
            LuthPipelineRootNode pipeline = new LuthPipelineRootNode(lang, scan, frameDescriptor);
            queryNode.add(0, pipeline);
            return scan;
        } else if (node instanceof ArrowScan) {
            LuthCallableOperator scan = createArrowScan(lang, (ArrowScan) node, next, frameDescriptor);
            LuthPipelineRootNode pipeline = new LuthPipelineRootNode(lang, scan, frameDescriptor);
            queryNode.add(0, pipeline);
            return scan;
        } else if (node instanceof UDFOperator) {
            LuthCallableOperator udf = processUDF(lang, (UDFOperator) node, frameDescriptor, next);
            return createPhysicalOperator(udf, lang, node.getParents().get(0), frameDescriptor, queryNode);
        } else if (node instanceof Sink) {
            LuthCallableOperator sink = createSink(lang, (Sink) node, frameDescriptor);
            return createPhysicalOperator(sink, lang, node.getParents().get(0), frameDescriptor, queryNode);

         else if (node instanceof PolyglotUDFOperator) {
            LuthCallableOperator poly = processPolyglotUDF((PolyglotUDFOperator) node, frameDescriptor, next);
            return createPhysicalOperator(poly, lang, node.getParents().get(0), frameDescriptor, queryNode);
       else if (node instanceof Projection) {
            LuthCallableOperator projection = createProjection(lang, (Projection) node, frameDescriptor, next);
            return createPhysicalOperator(projection, lang, node.getParents().get(0), frameDescriptor, queryNode);
        } else if (node instanceof Function) {
            LuthCallableOperator map = createMap(lang, (Function) node, frameDescriptor, next);
            return createPhysicalOperator(map, lang, node.getParents().get(0), frameDescriptor, queryNode);
        } else if (node instanceof Join) {
           // createJoin(lang, (Join) node, frameDescriptor, next, queryNode);
            return null;
            */
        else {
            throw new RuntimeException("Not Implemented");
        }

    }

    private void createJoin(BabelfishEngine lang, Join join, FrameDescriptor frameDescriptor, BFOperator next, ArrayList<BFPipelineRoot> pipelines) {

        Predicate.Equal equalPrediacte = (Predicate.Equal) join.getPredicate();
        FieldReference leftField = (FieldReference) equalPrediacte.getLeft();
        FieldReference rightField = (FieldReference) equalPrediacte.getRight();
        // 8000
        long card = join.getCardinality() == -1 ? 150000 : join.getCardinality();
        JoinContext joinContext = new JoinContext(card);
        BFJoinProbOperator joinProbe = new BFJoinProbOperator(lang, frameDescriptor, next, rightField, null, joinContext);
        createPhysicalOperator(joinProbe, lang, join.getParents().get(1), frameDescriptor, pipelines);

        BFJoinBuildOperator joinBuild = new BFJoinBuildOperator(lang, frameDescriptor, new KeyGroup(leftField), null, joinContext);
        createPhysicalOperator(joinBuild, lang, join.getParents().get(0), frameDescriptor, pipelines);
    }

    private BFOperator createGroupBy(TruffleLanguage<?> lang,
                                     GroupBy node,
                                     FrameDescriptor frameDescriptor,
                                     BFOperator next,
                                     ArrayList<BFPipelineRoot> pipelines) {
        Aggregation[] aggregations = node.getAggregations();
        AggregationNode[] aggNode = new AggregationNode[aggregations.length];
        StateDescriptor.Builder stateDescriptorBuilder = new StateDescriptor.Builder();
        if (node.hasKeys()) {
            for (FieldReference key : node.getKeys().getKeys()) {
                stateDescriptorBuilder.add(PhysicalFieldFactory.getPhysicalField(key.getType(), key.getName(), key.getMaxLength(), key.getMaxLength()), null);
            }
        }
        for (int i = 0; i < aggregations.length; i++) {
            Aggregation aggregation = aggregations[i];
            FieldReference fieldStamp = ((FieldReference) aggregation.getFieldStamp());
            if (aggregation instanceof Aggregation.Sum) {
                aggNode[i] = AggregationNode.SumNode.create(frameDescriptor, fieldStamp,
                        "sum_" + i,
                        frameDescriptor.findOrAddFrameSlot("statevar"));
            }
            if (aggregation instanceof Aggregation.Min) {
                aggNode[i] = AggregationNode.MinNode.create(frameDescriptor, fieldStamp,
                        "min_" + i,
                        frameDescriptor.findOrAddFrameSlot("statevar"));
            } else if (aggregation instanceof Aggregation.Count) {
                aggNode[i] = AggregationNode.CountNode.create("count_" + i, frameDescriptor.findOrAddFrameSlot("statevar"));
            }
            stateDescriptorBuilder.add(aggNode[i].getPhysicalField(), aggNode[i].getDefaultValue());
        }


        StateDescriptor stateDescriptor = stateDescriptorBuilder.build("aggregation");

        if (node.hasKeys()) {
            long card = node.getCardinality() == -1 ? 1500000 : node.getCardinality();
            AggregationContext context = new AggregationContext(card);
            BFAggregationScanOperator aggregationScanOperator = new BFAggregationScanOperator(lang, frameDescriptor, next, stateDescriptor, context);
            BFPipelineRoot aggPipeline = new BFPipelineRoot(aggregationScanOperator);
            pipelines.add(0, aggPipeline);

            if (RuntimeConfiguration.MULTI_THREADED) {
                throw new RuntimeException();
                // return new SyncLuthGroupByKey(lang, frameDescriptor, aggNode, node.getKeys(), stateDescriptor, context);
            } else {
                return new BFGroupByKeyOperator(lang, frameDescriptor, aggNode, node.getKeys(), stateDescriptor, context);
            }
        } else {
            AggregationContext context = new AggregationContext(-1);
            BFAggregationReadOperator aggegationReadNode = new BFAggregationReadOperator(lang, frameDescriptor, next, stateDescriptor, context);
            BFPipelineRoot aggPipeline = new BFPipelineRoot(aggegationReadNode);
            pipelines.add(0, aggPipeline);
            if (RuntimeConfiguration.MULTI_THREADED) {
                throw new RuntimeException();
                //return new SyncLuthGroupBy(lang, frameDescriptor, aggNode, stateDescriptor);
            } else {
                return new BFGroupByOperator(lang, frameDescriptor, aggNode, stateDescriptor, context);
            }

        }
    }


    private BFOperator createSelection(TruffleLanguage<?> lang, Selection node, FrameDescriptor frameDescriptor, BFOperator next) {

        PredicateNode predicateNode = createPredicate(lang, node.getPredicates()[0], frameDescriptor, next);
        return new BFSelectionOperator(lang, frameDescriptor, predicateNode, next);
    }


    private BFOperator createProjection(TruffleLanguage<?> lang, Projection node, FrameDescriptor frameDescriptor, BFOperator next) {
        return new BFProjectionOperator(lang, frameDescriptor, node.getFieldReferences(), next);
    }

    private BFOperator createMap(TruffleLanguage<?> lang, Function node, FrameDescriptor frameDescriptor, BFOperator next) {
        ExpressionNode expressionNode = (ExpressionNode) createExpression(lang, node.getExpression(), frameDescriptor, next);
        return new BFMapOperator(lang, frameDescriptor, next, expressionNode, node.getAs());
    }


    private BFExecutableNode createExpression(TruffleLanguage<?> lang, Function.Expression expression, FrameDescriptor frameDescriptor, BFOperator next) {

        if (expression instanceof Function.ValueExpression) {
            Function.ValueExpression valueExpression = (Function.ValueExpression) expression;
            return createPredicateItem(lang, valueExpression.getStamp(), frameDescriptor, next);
        }

        Function.BinaryExpression binaryExpression = (Function.BinaryExpression) expression;
        BFExecutableNode left = createExpression(lang, binaryExpression.getLeft(), frameDescriptor, next);
        BFExecutableNode right = createExpression(lang, binaryExpression.getRight(), frameDescriptor, next);

        switch (binaryExpression.getFunction()) {
            case Mul: {
                return ExpressionNodeFactory.MulExpressionNodeGen.create(lang, left, right);
            }
            case Add: {
                return ExpressionNodeFactory.AddExpressionNodeGen.create(lang, left, right);
            }
            case Min: {
                return ExpressionNodeFactory.SubExpressionNodeGen.create(lang, left, right);
            }
            case Div: {
                return ExpressionNodeFactory.DivExpressionNodeGen.create(lang, left, right);
            }
        }
        return null;
    }

    private PredicateNode createPredicate(TruffleLanguage<?> lang, Predicate p, FrameDescriptor frameDescriptor, BFOperator next) {

        if (p instanceof Predicate.And) {
            Predicate.And and = (Predicate.And) p;
            PredicateNode leftNode = createPredicate(lang, and.getLeft(), frameDescriptor, next);
            PredicateNode rightNode = createPredicate(lang, and.getRight(), frameDescriptor, next);
            return new PredicateNode.AndNode(leftNode, rightNode);
        } else if (p instanceof Predicate.Or) {
            Predicate.Or or = (Predicate.Or) p;
            PredicateNode leftNode = createPredicate(lang, or.getLeft(), frameDescriptor, next);
            PredicateNode rightNode = createPredicate(lang, or.getRight(), frameDescriptor, next);
            return new PredicateNode.OrNode(leftNode, rightNode);
        } else if (p instanceof Predicate.GreaterThan) {
            Predicate.GreaterThan greaterThan = (Predicate.GreaterThan) p;
            PredicateNode.PredicateItemNode leftNode = createPredicateItem(lang, greaterThan.getLeft(), frameDescriptor, next);
            PredicateNode.PredicateItemNode rightNode = createPredicateItem(lang, greaterThan.getRight(), frameDescriptor, next);
            return PredicateNode.GreaterThanNode.create(leftNode, rightNode);
        } else if (p instanceof Predicate.GreaterEquals) {
            Predicate.GreaterEquals greaterThan = (Predicate.GreaterEquals) p;
            PredicateNode.PredicateItemNode leftNode = createPredicateItem(lang, greaterThan.getLeft(), frameDescriptor, next);
            PredicateNode.PredicateItemNode rightNode = createPredicateItem(lang, greaterThan.getRight(), frameDescriptor, next);
            return PredicateNode.GreaterEqualsNode.create(leftNode, rightNode);
        } else if (p instanceof Predicate.LessThen) {
            Predicate.LessThen lessThen = (Predicate.LessThen) p;
            PredicateNode.PredicateItemNode leftNode = createPredicateItem(lang, lessThen.getLeft(), frameDescriptor, next);
            PredicateNode.PredicateItemNode rightNode = createPredicateItem(lang, lessThen.getRight(), frameDescriptor, next);
            return PredicateNode.LessThanNode.create(leftNode, rightNode);
        } else if (p instanceof Predicate.LessEquals) {
            Predicate.LessEquals lessThen = (Predicate.LessEquals) p;
            PredicateNode.PredicateItemNode leftNode = createPredicateItem(lang, lessThen.getLeft(), frameDescriptor, next);
            PredicateNode.PredicateItemNode rightNode = createPredicateItem(lang, lessThen.getRight(), frameDescriptor, next);
            return PredicateNode.LessEqualsNode.create(leftNode, rightNode);
        } else if (p instanceof Predicate.Equal) {
            Predicate.Equal equal = (Predicate.Equal) p;
            PredicateNode.PredicateItemNode leftNode = createPredicateItem(lang, equal.getLeft(), frameDescriptor, next);
            PredicateNode.PredicateItemNode rightNode = createPredicateItem(lang, equal.getRight(), frameDescriptor, next);
            return PredicateNode.EqualNode.create(leftNode, rightNode);
        }
        throw new IllegalArgumentException();
    }

    private PredicateNode.PredicateItemNode createPredicateItem(TruffleLanguage<?> lang, FieldStamp p, FrameDescriptor frameDescriptor, BFOperator next) {

        if (p instanceof FieldConstant) {
            FieldConstant c = (FieldConstant) p;
            return PredicateNodeFactory.ConstantValueNodeGen.create(lang, c.getValue());
        } else if (p instanceof FieldReference) {
            return new PredicateNode.ReadFieldNode(((FieldReference) p).getKey(), frameDescriptor, lang);

        }
        throw new IllegalArgumentException();
    }

    public BFOperatorInterface createScan(BabelfishEngine lang, Scan logicalScan, BFOperator next, FrameDescriptor frameDescriptor) {

        //  return new BFParallelScanOperator(lang, frameDescriptor, next,logicalScan.catalogName);
        return new BFScanOperator(lang, frameDescriptor, logicalScan.catalogName, next);
    }

    public BFOperatorInterface createParallelScan(BabelfishEngine lang, ParallelScan logicalScan, BFOperator next, FrameDescriptor frameDescriptor) {
        return new BFParallelScanOperator(lang, frameDescriptor, next, logicalScan.catalogName, logicalScan.threads, logicalScan.chunkSize);
    }

    public BFOperatorInterface createCSVScan(BabelfishEngine lang,
                                             CSVScan logicalScan,
                                             BFOperator next,
                                             FrameDescriptor frameDescriptor) {
        return new BFCSVScanOperator(lang, frameDescriptor, logicalScan.catalogName, next);
    }

    public BFOperatorInterface createArrowScan(BabelfishEngine lang, ArrowScan logicalScan, BFOperator next, FrameDescriptor frameDescriptor) {
        return new BFArrowScanOperator(lang, frameDescriptor, logicalScan.catalogName, next);
    }

    private BFOperator createSink(BabelfishEngine lang, Sink logicalSink, FrameDescriptor frameDescriptor) {
        if (logicalSink instanceof Sink.PrintSink)
            return new BFPrintSink(lang, frameDescriptor);
        else
            return new BFMemorySink(lang, frameDescriptor);
    }

    public BFOperator processUDF(TruffleLanguage<?> lang, UDFOperator udfOperator, FrameDescriptor frameDescriptor, BFOperator nextOperator) {
        if (udfOperator instanceof JavaTypedUDFOperator)
            return new BFJavaTypedUDFOperator(lang, frameDescriptor, (JavaTypedUDFOperator) udfOperator, nextOperator);
        return new BFJavaUDFOperator(lang, frameDescriptor, udfOperator, nextOperator);
    }

    public BFOperatorInterface processPolyglotUDF(BabelfishEngine lang, PolyglotUDFOperator udfOperator, FrameDescriptor frameDescriptor, BFOperator nextOperator) {

        if (udfOperator.getUdf() instanceof ScalarUDF) {
            if (udfOperator.getUdf().getLanguage().equals("python")) {
                if (udfOperator.getUdf() instanceof PythonPandasUDF) {
                    return new BFScalarUDFOperatorForPythonPandas(lang, frameDescriptor, babelfishContext, udfOperator, nextOperator);
                }
                return new BFScalarUDFOperatorForPython(lang, frameDescriptor, babelfishContext, udfOperator, nextOperator);
            }
            return new BFScalarUDFOperator(lang, frameDescriptor, babelfishContext, udfOperator, nextOperator);
        } else if (udfOperator.getUdf() instanceof SelectionUDF) {
            return new BFSelectionUDFOperator(lang, frameDescriptor, babelfishContext, udfOperator, nextOperator);
        } else if (udfOperator.getUdf() instanceof TransformUDF) {
            return new BFTransformPolyglotUDFNode(lang, frameDescriptor, babelfishContext, udfOperator, nextOperator);
        } else {
            throw new RuntimeException();
        }

    }


}
