package de.tub.dima.babelfish.typesytem.variableLengthType;

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
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.library.GenerateLibrary;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.storage.text.PointerText;

@ExportLibrary(value = InteropLibrary.class, receiverType = EagerText.class)
@ExportLibrary(value = TextLibrary.class, receiverType = EagerText.class)
public class EagerTextBuiltins {

    @ExportMessage
    static char getChar(EagerText text, int index){
        return text.get(index);
    }

    @ExportMessage
    static Object getMembers(EagerText receiver, boolean includeInternal) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    public static boolean isNull(EagerText type) {
        return false;
    }

    @ExportMessage
    public static boolean hasMembers(EagerText type) {
        return true;
    }

    @ExportMessage
    public static boolean isMemberInvocable(EagerText type, String member) {
        return true;
    }

    public static String getStringArg(Object[] arguments) {
        return (String) arguments[0];
    }


    public static BuildInFunctionNode createNode(String member) {
        switch (member) {
            case "equals":
                return EqualsTextNode.create();
            case "reverse":
                return ReverseTextNode.create();
            case "substring":
                return SubstringNode.create();
            case "concat":
                return ConcatTextNode.create();
            case "split":
                return SplitTextNode.create();
            case "uppercase":
                return UppercaseTextNode.create();
            case "lowercase":
                return LowercaseTextNode.create();
            case "asString":
                return ToStringNode.create();

        }
        return null;
    }

    @ExportMessage
    public static class invokeMember {
        @Specialization
        public static Object invokeMember(EagerText type, String member, Object[] arguments, @Cached(value = "createNode(member)", allowUncached = true) BuildInFunctionNode node) {
            if (arguments.length == 0) {
                return node.call(type);
            } else if (arguments.length == 1) {
                return node.call(type, arguments[0]);
            } else {
                return node.call(type, arguments[0], arguments[1]);
            }
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
    @NodeChild(type = ReadArgumentNode.class)
    static abstract class ConcatTextNode extends BuildInFunctionNode {

        public static ConcatTextNode create() {
            return EagerTextBuiltinsFactory.ConcatTextNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1));
        }

        public static StringText getConstantRope(String string) {
            return new StringText(string);
        }

        @Specialization()
        public static StringText concat(EagerText date, String string) {
            return new StringText(date.toString() + string, date.length() + string.length());
        }

    }

    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    public static abstract class EqualsTextNode extends BuildInFunctionNode {

        public static EqualsTextNode create() {
            return EagerTextBuiltinsFactory.EqualsTextNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1));
        }

        @Specialization
        public static boolean equals(EagerText date, StringText otherDate) {
            return date.equals(otherDate);
        }

        @ExplodeLoop
        public static boolean equalsLoop(EagerText first, EagerText second, int size) {
            for (int i = 0; i < size; i++) {
                if(first.get(i) != second.get(i)){
                    return false;
                };
            }
            return true;
        }

        public static StringText getConstantRope(int length, String string) {
            return new StringText(string, length);
        }

        @Specialization(guards = "string == cached_string")
        public static boolean equals(EagerText date, String string, @Cached("string") String cached_string,
                                     @Cached("getConstantRope(date.length(), string)") StringText value) {
            if (date.length() == value.length()) {
                return equalsLoop(date, value, value.length());
            }
            return false;

        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    static abstract class ReverseTextNode extends BuildInFunctionNode {

        public static ReverseTextNode create() {
            return EagerTextBuiltinsFactory.ReverseTextNodeGen.create(new ReadArgumentNode(0));
        }

        @Specialization
        public static Text reverse(EagerText rope) {
            return rope.reverse();
        }
    }


    @NodeChild(type = ReadArgumentNode.class)
    static abstract class UppercaseTextNode extends BuildInFunctionNode {

        public static UppercaseTextNode create() {
            return EagerTextBuiltinsFactory.UppercaseTextNodeGen.create(new ReadArgumentNode(0));
        }

        @Specialization
        public static Text uppercase(EagerText rope) {
            return rope.uppercase();
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    static abstract class ToStringNode extends BuildInFunctionNode {

        public static ToStringNode create() {
            return EagerTextBuiltinsFactory.ToStringNodeGen.create(new ReadArgumentNode(0));
        }

        @Specialization
        public static String toString(PointerText rope) {
            return rope.toString();
        }
        public static String toString(EagerText rope) {
            return rope.toString();
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    static abstract class LowercaseTextNode extends BuildInFunctionNode {

        public static LowercaseTextNode create() {
            return EagerTextBuiltinsFactory.LowercaseTextNodeGen.create(new ReadArgumentNode(0));
        }

        @Specialization
        public static Text uppercase(EagerText rope) {
            return rope.lowercase();
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    static abstract class SubstringNode extends BuildInFunctionNode {

        public static SubstringNode create() {
            return EagerTextBuiltinsFactory.SubstringNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1), new ReadArgumentNode(2));
        }

        @Specialization
        public static Text getChild(EagerText date, int start, int end) {
            return date.substring(start, end);
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    static abstract class SplitTextNode extends BuildInFunctionNode {

        public static SplitTextNode create() {
            return EagerTextBuiltinsFactory.SplitTextNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1));
        }


        @Specialization()
        @CompilerDirectives.TruffleBoundary
        public static StringTextArray split(EagerText date,
                                             String string) {
            String[] array = date.toString().split(string);
            StringText[] res = new StringText[array.length];
            for (int i = 0; i < res.length; i++) {
                res[i] = new StringText(array[i]);
            }
            return new StringTextArray(res);
        }
    }

    @ExportMessage
    public static boolean isString(EagerText receiver) {
        return true;
    }


    @ExportMessage
    public static String asString(EagerText receiver) throws UnsupportedMessageException {
        return receiver.toString();
    }

    @ExportMessage
    static boolean hasLanguage(EagerText text) {
        return false;
    }
    @ExportMessage static Class<TruffleLanguage<?>> getLanguage(EagerText text) throws UnsupportedMessageException { return null; }
    @ExportMessage static Object toDisplayString(EagerText text, boolean allowSideEffects) { return null; }
}
