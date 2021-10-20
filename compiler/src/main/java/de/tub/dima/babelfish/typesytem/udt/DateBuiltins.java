package de.tub.dima.babelfish.typesytem.udt;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;

import java.time.LocalDate;

@ExportLibrary(value = InteropLibrary.class, receiverType = AbstractDate.class)
@ExportLibrary(value = DateLibrary.class, receiverType = AbstractDate.class)
public class DateBuiltins {


    @ExportMessage
    public static class getTs {
        @Specialization
        public static int getTs(Date date) {
            return date.getUnixTs();
        }

        @Specialization
        public static int getTs(ArrowSourceDate date) {
            return date.getUnixTs();
        }

        @Specialization
        public static int getTs(LazyDate date) {
            return date.getUnixTs();
        }

        @Specialization(guards = "cached")
        public static int getValue(CSVSourceDate value, @Cached("value.cached") boolean cached) {
            return value.getCachedValue();
        }

        @Specialization
        public static int getTs(CSVSourceDate date) {
            return date.getUnixTs();
        }
    }

    @ExportMessage
    public static class getYear {
        @Specialization
        public static int getYear(AbstractDate date, @CachedLibrary(value = "date") DateLibrary dateLibrary) {
            int unixTs = dateLibrary.getTs(date);
           // return (unixTs / 31556926) + 1970;
            return unixTs /10000;
        }
    }

    @ExportMessage
    static Object getMembers(AbstractDate receiver, boolean includeInternal) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    public static boolean hasMembers(AbstractDate type) {
        return true;
    }

    @ExportMessage
    public static boolean isMemberInvocable(AbstractDate type, String member) {
        return true;
    }

    public static String getStringArg(Object[] arguments) {
        return (String) arguments[0];
    }


    @ExportMessage
    public static boolean isBfNode(AbstractDate object) {
        return true;
    }

    @ExportMessage
    public static class ExecuteBinaryOperation {
        @Specialization()
        public static Object executeBinaryOperation(AbstractDate object,
                                                     AbstractDate other,
                                                     String method,
                                                     @Cached(value = "createNode(method)", allowUncached = true) BuildInFunctionNode node) {
            return node.call(object, other);
        }
    }


    public static BuildInFunctionNode createNode(String member) {
        switch (member) {
            case ">":
                return AfterDateNode.create();
            case "<":
                return BeforeDateNode.create();
            case "after":
                return AfterDateNode.create();
            case "before":
                return BeforeDateNode.create();
            case "asTs":
                return GetUnixTsNode.create(new ReadArgumentNode(0));
            case "year":
                return GetYearNode.create(new ReadArgumentNode(0));
        }
        return null;
    }

    @ExportMessage
    public static class InvokeMember {

        @Specialization(guards = "arguments.length == 0")
        public static Object invokeMemberWithoutArgs(AbstractDate type, String member,
                                                     Object[] arguments,
                                                     @Cached(value = "createNode(member)", allowUncached = true) BuildInFunctionNode node) {
            return node.call(type);
        }

        @Specialization()
        public static Object invokeMember(AbstractDate type, String member,
                                          Object[] arguments,
                                          @Cached(value = "createNode(member)", allowUncached = true) BuildInFunctionNode node) {
            return node.call(type, arguments[0]);
        }
    }

    static class ReadArgumentNode extends Node {
        private final int argument;

        public ReadArgumentNode(int argument) {
            this.argument = argument;
        }

        public Object execute(VirtualFrame frame) {
            return frame.getArguments()[argument];
        }
    }

    static abstract class BuildInFunctionNode extends Node {

        @CompilerDirectives.CompilationFinal
        private final FrameDescriptor localFrameDescriptor;

        BuildInFunctionNode() {
            localFrameDescriptor = new FrameDescriptor();
        }

        public abstract Object execute(VirtualFrame frame);


        public Object call(Object... arguments) {
            VirtualFrame virtualFrame = Truffle.getRuntime().createVirtualFrame(arguments, localFrameDescriptor);
            return execute(virtualFrame);
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    static abstract class GetYearNode extends BuildInFunctionNode {

        public static GetYearNode create(ReadArgumentNode readArgumentNode) {
            return DateBuiltinsFactory.GetYearNodeGen.create(readArgumentNode);
        }

        @Specialization()
        public static int getYear(AbstractDate date, @CachedLibrary(limit = "30") DateLibrary dateLibrary) {
            return dateLibrary.getYear(date);
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    static abstract class GetUnixTsNode extends BuildInFunctionNode {

        public static GetUnixTsNode create(ReadArgumentNode readArgumentNode) {
            return DateBuiltinsFactory.GetUnixTsNodeGen.create(readArgumentNode);
        }

        public static Date createDate(String date) {
            return new Date(date);
        }

        @Specialization(guards = "string == cached_string", limit = "10")
        public static long getUnixTsFromString(String string, @Cached("string") String cached_string,
                                               @Cached("createDate(cached_string)") Date cachedDate,
                                               @Cached("cachedDate.unixTs") long cached_ts) {
            return cached_ts;
        }

        @Specialization()
        public static long getUnixTs(String string) {
            return createDate(string).unixTs;
        }

        @Specialization()
        public static long getUnixTs(Date date) {
            return date.unixTs;
        }

        @Specialization()
        public static long getUnixTs(LazyDate date) {
            return date.getUnixTs();
        }

        @Specialization()
        public static long getUnixTs(AbstractDate date) {
            return date.getUnixTs();
        }

    }

    @NodeChild(type = GetUnixTsNode.class)
    @NodeChild(type = GetUnixTsNode.class)
    static abstract class AfterDateNode extends BuildInFunctionNode {


        public static AfterDateNode create() {
            return DateBuiltinsFactory.
                    AfterDateNodeGen.
                    create(GetUnixTsNode.create(new ReadArgumentNode(0)), GetUnixTsNode.create(new ReadArgumentNode(1)));
        }

        @Specialization()
        public static boolean afterDate(long unixTs1, long unixTs2) {
            return unixTs1 >= unixTs2;
        }

    }

    @NodeChild(type = GetUnixTsNode.class)
    @NodeChild(type = GetUnixTsNode.class)
    static abstract class BeforeDateNode extends BuildInFunctionNode {


        public static Date createDate(String date) {
            return new Date(date);
        }

        public static BeforeDateNode create() {
            return DateBuiltinsFactory.
                    BeforeDateNodeGen.
                    create(GetUnixTsNode.create(new ReadArgumentNode(0)), GetUnixTsNode.create(new ReadArgumentNode(1)));
        }


        @Specialization()
        public static boolean beforeDate(long unixTs1,
                                         long unixTs2) {
            return unixTs1 < unixTs2;
        }

    }



    @ExportMessage
    static boolean hasLanguage(AbstractDate re) {
        return false;
    }

    @ExportMessage
    static boolean isDate(AbstractDate re) {
        return true;
    }

    @ExportMessage
    static LocalDate asDate(AbstractDate re) {
        return LocalDate.ofEpochDay(re.getUnixTs());
    }

    @ExportMessage
    static Class<TruffleLanguage<?>> getLanguage(AbstractDate re) throws UnsupportedMessageException {
        return null;
    }

    @ExportMessage
    static Object toDisplayString(AbstractDate re, boolean allowSideEffects) {
        return re.toString();
    }
}
