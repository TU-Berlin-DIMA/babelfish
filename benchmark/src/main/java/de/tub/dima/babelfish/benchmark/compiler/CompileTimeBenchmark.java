package de.tub.dima.babelfish.benchmark.compiler;

import de.tub.dima.babelfish.BufferArgument;
import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkConfig;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.BenchmarkUtils;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.benchmark.tcph.RelationalBenchmarkRunner;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeRawResultFile;

public class CompileTimeBenchmark extends AbstractBenchmark {

    CompileTimeBenchmark(BenchmarkConfig config) throws IOException, SchemaExtractionException, InterruptedException {
        TCPHImporter.importTCPH("/tpch/");
        LogicalOperator queryPlan = RelationalBenchmarkRunner.getQueryPlan(config);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        System.err.println("***");
        System.err.println("***");
        System.err.println("Start Compiletime Benchmark: " + config.query + " in " + config.language + " With LazyText=" + config.lazy_string_handling);
        System.err.println("***");
        System.err.println("***");
        executeBenchmark(queryPlan, 200, 0);
        executeBenchmark(queryPlan, 200, 0);
        executeBenchmark(queryPlan, 200, 0);
        //MemoryMonitor m = new MemoryMonitor();
        //Thread t = new Thread(m);
        //t.start();
        BenchmarkResults results = executeBenchmark(queryPlan, 0, 10_000);
        writeRawResultFile("CompileTimeBenchmark", config, results);
    }

    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        BenchmarkConfig config = BenchmarkUtils.parseConfig(args);

        new CompileTimeBenchmark(config);
    }

    protected BenchmarkResults executeBenchmark(LogicalOperator query, int warmup, int runtime) throws InterruptedException {
        Value executableQuery = submitQuery(new LogicalQueryPlan((Sink) query));

        for (int i = 0; i < warmup; i++) {
            executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
        }
        //System.gc();
        BenchmarkResults results = new BenchmarkResults();
        long startTs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTs < runtime) {
            long outerStart = System.currentTimeMillis();
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            long externalRuntime = System.currentTimeMillis() - outerStart;

            System.out.println("Execution Time:" + time + ("Time2: ") + (externalRuntime));
            BenchmarkResults.BenchmarkResult result = new BenchmarkResults.BenchmarkResult(time.asLong(), externalRuntime);
            results.add(result);
        }
        return results;
    }

    protected Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth", "js", "python")
                    //.option("engine.LanguageAgnosticInlining", "false")
                    //.option("engine.Mode", "latency")
                    .option("engine.Mode", "throughput")
                    //.option("engine.OSR", "false")
                    //.option("engine.OSRCompilationThreshold", "1000")
                    //.option("engine.CompilationThreshold", "5")
                    //.option("engine.TracePerformanceWarnings", "all")
                    //.option("engine.InstrumentBranches", "true")
                    //.option("engine.EncodedGraphCacheCapacity", "-1")
                    .option("engine.IterativePartialEscape", "true")
                    //.option("engine.MultiTier", "true")
                    //.option("engine.Inlining", "false")
                    // .("engine.PartialEscapeAnalysis", "true")
                    //.option("engine.CompileImmediately", "true")
                    //.option("engine.Compilation", "false")
                    // .option("engine.TraceCompilation", "true")
                    //.option("engine.TraceCompilationDetails", "true")
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

    public static class MemoryMonitor implements Runnable {

        public boolean running = true;

        int i = 0;

        @Override
        public void run() {
            while (running) {
                long heapSize = Runtime.getRuntime().totalMemory();
                i++;
// Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
                if (i % 100 == 0)
                    System.gc();
                MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
                MemoryUsage heapMemoryUsage = memBean.getHeapMemoryUsage();
                // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
                double heapFreeSize = heapMemoryUsage.getUsed();
                double usedInMb = heapFreeSize / 1024 / 1024;
                System.out.println(" used: " + usedInMb + " MB");
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
