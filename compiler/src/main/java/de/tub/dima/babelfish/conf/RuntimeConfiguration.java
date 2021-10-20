package de.tub.dima.babelfish.conf;

import com.oracle.truffle.api.CompilerDirectives;

public class RuntimeConfiguration {

    public static boolean ELIMINATE_EMPTY_IF_PROFILING = false;
    public static boolean LAZY_STRING_HANDLING = false;
    public static boolean NAIVE_STRING_HANDLING = false;
    public static boolean LAZY_READS = true;
    public static boolean LAZY_PARSING = true;
    @CompilerDirectives.CompilationFinal
    public static boolean MULTI_THREADED = false;
    public static boolean REPLACE_BY_CAS_LOOP = false;
    public static boolean REPLACE_BY_ATOMIC = false;
    public static boolean ELIMINATE_EMPTY_IF = false;
    public static boolean ELIMINATE_FILTER_IF = false;
    public static boolean REPLACE_WITH_PREAGGREGATION = false;
}
