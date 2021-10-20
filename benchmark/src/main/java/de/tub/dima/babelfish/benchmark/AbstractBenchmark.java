package de.tub.dima.babelfish.benchmark;

import de.tub.dima.babelfish.BufferArgument;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public abstract class AbstractBenchmark {

    protected Buffer buffer;
    protected BufferManager outputBufferManager;

    public AbstractBenchmark() {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");
        outputBufferManager = new BufferManager();
    }

    protected BenchmarkResults executeBenchmark(LogicalOperator query, int warmup, int iterations) throws InterruptedException {
        Value executableQuery = submitQuery(new LogicalQueryPlan((Sink) query));

        for (int i = 0; i < warmup; i++) {
            executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
        }
        BenchmarkResults results = new BenchmarkResults();
        for (int i = 0; i < iterations; i++) {
            long outerStart = System.currentTimeMillis();
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            long externalRuntime = System.currentTimeMillis() - outerStart;
            System.out.println("Execution Time:" + time + ("Time2: ") + (externalRuntime));
            if (externalRuntime >= 0) {
                BenchmarkResults.BenchmarkResult result = new BenchmarkResults.BenchmarkResult(time.asLong(), externalRuntime);
                results.add(result);
            }
        }
        return results;
    }

    protected Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth", "js", "python")
                    //.option("engine.LanguageAgnosticInlining", "false")
                    .option("engine.IterativePartialEscape", "true")
                    .option("engine.MultiTier", "false")
                    //.option("engine.TraceTransferToInterpreter", "true")
                    //.option("engine.Inlining", "false")
                    //.option("engine.CompileImmediately", "true")
                    //.option("engine.Compilation", "false")
                    //  .option("engine.TraceCompilation", "true")
                    //  .option("engine.TraceCompilationDetails", "true")
                    //  .option("engine.CompilationExceptionsArePrinted", "true")

                    .option("python.NoAsyncActions", "true")
                    .option("python.SysPrefix", "/home/pgrulich/projects/luth-org/luth/graalpython_venv")
                    .option("python.SysBasePrefix", "/home/pgrulich/tools/java/graalvm-ce-java8-20.3.0/jre/languages/python")
                    .option("python.StdLibHome", "/home/pgrulich/tools/java/graalvm-ce-java8-20.3.0/jre/languages/python/lib-python/3")
                    .option("python.CAPI", "/home/pgrulich/tools/java/graalvm-ce-java8-20.3.0/jre/languages/python/lib-graalpython")

                    // default 150000
                    .option("engine.MaximumInlineNodeCount", "800000")
                    // defualt 30_000
                    .option("engine.InliningExpansionBudget", "200000")
                    // defualt 30_000
                    .option("engine.InliningInliningBudget", "200000")
                    // default 400000
                    .option("engine.MaximumGraalNodeCount", "800000")

                    //.option("engine.CompilationStatistics", "true")
                    //.option("engine.CompilationStatisticDetails", "true")
                    //.option("engine.ShowInternalStackFrames", "true")
                    .allowAllAccess(true).build();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(plan);
            oos.flush();
            oos.close();
            Source s = Source.newBuilder("luth", ByteSequence.create(baos.toByteArray()), "testPlan").build();
            Value pipeline = context.eval(s);
            return pipeline;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
