package de.tub.dima.babelfish.benchmark.compiler;

import de.tub.dima.babelfish.benchmark.AbstractBenchmark;
import de.tub.dima.babelfish.benchmark.BenchmarkResults;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.benchmark.tcph.queries.Query6;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
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

import static de.tub.dima.babelfish.benchmark.BenchmarkUtils.writeResultFile;

public class CompileOptionBenchmark extends AbstractBenchmark {

    private final CompilerBenchmarkConfig config;

    CompileOptionBenchmark(CompilerBenchmarkConfig config) throws IOException, SchemaExtractionException, InterruptedException {
        TCPHImporter.importTCPH("/home/pgrulich/projects/luth-org/tpch-dbgen/");
        this.config = config;
        LogicalOperator queryPlan = Query6.getExecution(config.language);
        RuntimeConfiguration.LAZY_STRING_HANDLING = true;
        System.err.println("***");
        System.err.println("***");
        System.err.println("Start Compile Option Benchmark: " + config.query);
        System.err.println("***");
        System.err.println("***");
        BenchmarkResults results = executeBenchmark(queryPlan, 50, 50);
        writeResultFile("CompileOptionBenchmark", config, results);
    }

    public static void main(String[] args) throws IOException, SchemaExtractionException, InterruptedException {
        CompilerBenchmarkConfig config = parseConfig(args);

        new CompileOptionBenchmark(config);
    }

    public static CompilerBenchmarkConfig parseConfig(String[] args) {
        return new CompilerBenchmarkConfig(args[0], args[1], Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]), Boolean.parseBoolean(args[5]));
    }

    protected Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context.Builder context = Context.newBuilder("luth", "js", "python")
                    //.option("engine.LanguageAgnosticInlining", "false")
                    //.option("engine.Mode", "latency")
                    //.option("engine.Mode", "throughput")
                    //.option("engine.OSR", "false")
                    //.option("engine.OSRCompilationThreshold", "10")
                    //.option("engine.TracePerformanceWarnings", "all")
                    .option("engine.TraceMethodExpansion", "true")
                    .option("engine.TraceCompilationAST", "true")
                    .option("engine.EncodedGraphCacheCapacity", "-1")


                    //.option("engine.MultiTier", "false")
                    //.option("engine.Inlining", "false")
                    // .("engine.PartialEscapeAnalysis", "true")
                    //.option("engine.CompileImmediately", "true")
                    //.option("engine.Compilation", "false")
                    // .option("engine.TraceCompilation", "true")
                    //.option("engine.TraceCompilationDetails", "true")
                    //.option("engine.CompilationStatistics", "true")
                    //.option("engine.CompilationStatisticDetails", "true")
                    //.option("engine.ShowInternalStackFrames", "true")
                    .option("engine.IterativePartialEscape", "true")
                    .allowAllAccess(true);
            if (config.truffleIterativePartialEscape) {
                context.option("engine.IterativePartialEscape", "true");
                System.out.println("ENABLE: IterativePartialEscape");
            } else {
                System.out.println("DISABLE: IterativePartialEscape");
            }

            if (!config.truffleInlining) {
                context.option("engine.Inlining", "false");
                System.out.println("DISABLE: Inlining");
            } else {
                System.out.println("ENABLE: Inlining");
            }

            if (!config.truffleCompilation) {
                context.option("engine.Compilation", "false");
                System.out.println("DISABLE: Compilation");
            } else {
                System.out.println("ENABLE: Compilation");
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(plan);
            oos.flush();
            oos.close();
            Source s = Source.newBuilder("luth", ByteSequence.create(baos.toByteArray()), "testPlan").build();
            Value pipeline = context.build().eval(s);
            return pipeline;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static class CompilerBenchmarkConfig {
        public final String query;
        public final String language;
        public final boolean truffleInlining;
        public final boolean truffleIterativePartialEscape;
        public final boolean truffleCompilation;
        public final boolean loopOpt;


        public CompilerBenchmarkConfig(String query, String language, boolean truffleInlining, boolean truffleIterativePartialEscape, boolean truffleCompilation, boolean loopOpt) {
            this.query = query;
            this.language = language;
            this.truffleInlining = truffleInlining;
            this.truffleIterativePartialEscape = truffleIterativePartialEscape;
            this.truffleCompilation = truffleCompilation;
            this.loopOpt = loopOpt;
        }
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
