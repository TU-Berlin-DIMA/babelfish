#!/usr/bin/env bash

MAIN_CLASS="de.tub.dima.luth.benchmark.compiler.CompileOptionBenchmark"
JDK_HOME="/home/pgrulich/tools/java/graalvm-ce-java8-20.3.0"
JAVA_HOME="$JDK_HOME/bin/java"

JAVA_ARGS=("-server" "-XX:+UnlockExperimentalVMOptions" "-XX:+EnableJVMCI" "-XX:+EagerJVMCI" "-XX:-UseJVMCINativeLibrary" "-XX:-UseJVMCIClassLoader" "-d64" "-ea" "-Djava.awt.headless=true" "-XX:-UseCompressedOops")
GRAAL_ARGS=("-Dgraal.DebugStubsAndSnippets=true" "-XX:-TraceDeoptimization" "-Dgraal.GenLoopSafepoints=false" "-Dgraal.ShowConfiguration=verbose")


PROGRAM_ARGS=()

for opt in "$@"
    do
      case $opt in
        -noRead)
           JAVA_ARGS+=("-Dgraal.OptReadElimination=false" "-Dgraal.OptFloatingReads=false") ;;
        -noLoop)
           JAVA_ARGS+=("-Dgraal.LoopPeeling=false" "-Dgraal.FullUnroll=false" "-Dgraal.LoopUnswitch=false" "-Dgraal.PartialUnroll=false") ;;
        -noPartialEscape)
            JAVA_ARGS+=("-Dgraal.PartialEscapeAnalysis=false" "-Dgraal.EscapeAnalysisIterations=1") ;;
        -dump)
            JAVA_ARGS+=("-Dgraal.Dump=Truffle:2"  "-Dgraal.PrintGraph=Network") ;;
        -disassemble)
            JAVA_ARGS+=("-Dgraal.PrintCFG=true" "-Dgraal.Dump" "-XX:+UnlockDiagnosticVMOptions" "-XX:CompileCommand=print,*LuthPipelineRootNode" "-XX:CompileCommand=exclude,*OptimizedCallTarget.callRoot" "-Dgraal.TruffleBackgroundCompilation=false" "-Dgraal.TraceTruffleCompilation=true" "-Dgraal.TraceTruffleCompilationDetails=true") ;;
        *)
            PROGRAM_ARGS+=("$opt") ;;
      esac
    done

Xbootclasspath="-Xbootclasspath/p:conf/target/classes:compiler/target/classes:operator/target/classes:storage/target/classes:typesystem/target/classes:javadriver/target/classes:js-driver/target/classes:benchmark/target/classes"
GraalPhython="$JDK_HOME/jre/languages/python/graalpython.jar"
ClassPath="-classpath benchmark/target/classes:operator/target/classes::typesystem/target/classes:compiler/target/classes:storage/target/classes:javadriver/target/classes:js-driver/target/classes:$GraalPhython"


"$JAVA_HOME" "${JAVA_ARGS[@]}" "${GRAAL_ARGS[@]}" $Xbootclasspath $ClassPath $MAIN_CLASS "${PROGRAM_ARGS[@]}" -luth.home=~ -js.home=~ --tool:regex
