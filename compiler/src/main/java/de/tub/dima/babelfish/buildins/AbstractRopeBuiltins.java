package de.tub.dima.babelfish.buildins;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Fallback;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.utilities.TriState;
import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.storage.text.Rope;
import de.tub.dima.babelfish.storage.text.leaf.*;
import de.tub.dima.babelfish.storage.text.operations.*;
import de.tub.dima.babelfish.typesytem.variableLengthType.EagerText;
import de.tub.dima.babelfish.typesytem.variableLengthType.TextLibrary;

@ExportLibrary(value = InteropLibrary.class, receiverType = AbstractRope.class)
@ExportLibrary(value = TextLibrary.class, receiverType = AbstractRope.class)
public class AbstractRopeBuiltins {

    @ExportMessage(library = TextLibrary.class)
    static class getChar{

        public static GetCharTextNode getCharNode() {
            return GetCharTextNode.create();
        }

        @Specialization
        public static char getChar(AbstractRope rope, int index, @Cached(value = "getCharNode()", allowUncached = true) GetCharTextNode node) {
            return (char) node.call(rope, index);
        }

    }

    @ExportMessage
    static Object getMembers(AbstractRope receiver, boolean includeInternal) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    public static boolean isNull(AbstractRope type) {
        return false;
    }

    @ExportMessage
    public static boolean hasMembers(AbstractRope type) {
        return true;
    }

    @ExportMessage
    public static boolean isMemberInvocable(AbstractRope type, String member) {
        return true;
    }

    @ExportMessage
    public static boolean hasArrayElements(AbstractRope rope) {
        return true;
    }

    @ExportMessage
    public static Object readArrayElement(AbstractRope rope, long index) throws UnsupportedMessageException, InvalidArrayIndexException {
        return rope;
    }

    @ExportMessage
    public static long getArraySize(AbstractRope receiver) {
        return 10;
    }

    @ExportMessage
    public static boolean isArrayElementReadable(AbstractRope receiver, long index) {
        return true;
    }

    public static String getStringArg(Object[] arguments) {
        return (String) arguments[0];
    }


    @ExportMessage
    public static TriState isIdenticalOrUndefined(AbstractRope receiver, Object other) {
        return TriState.TRUE;
    }

    @ExportMessage
    public static class IsIdentical {
        @Specialization
        public static boolean isIdentical(AbstractRope receiver, String other, InteropLibrary otherInterop, @Cached(value = "createEquals()", allowUncached = true) BuildInFunctionNode equals) {
            return (boolean) equals.call(receiver, other);
        }

        @Fallback
        public static boolean isIdentical(AbstractRope receiver, Object other, InteropLibrary otherInterop) {
            return false;
        }
    }


    @ExportMessage
    public static int identityHashCode(AbstractRope receiver) {
        return 0;
    }

    public static BuildInFunctionNode createEquals() {
        return createNode("equals");
    }

    public static BuildInFunctionNode createNode(String member) {
        switch (member) {
            case "get":
                return GetCharTextNode.create();
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
            case "lowercase":
                return LowercaseTextNode.create();
            case "uppercase":
                return UppercaseTextNode.create();
        }
        return null;
    }

    @ExportMessage
    public static class invokeMember {
        @Specialization
        public static Object invokeMember(AbstractRope type, String member, Object[] arguments, @Cached(value = "createNode(member)", allowUncached = true) BuildInFunctionNode node) {
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
    static abstract class EqualsTextNode extends BuildInFunctionNode {

        public static EqualsTextNode create() {
            return AbstractRopeBuiltinsFactory.EqualsTextNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1));
        }

        @Specialization
        public static boolean equals(AbstractRope date, AbstractRope otherDate) {
            return date.equals(otherDate);
        }

        @ExplodeLoop
        public static boolean equalsLoop(Rope first, Rope second, int size) {
            for (int i = 0; i < size; i++) {
                if (first.get(i) != second.get(i)) {
                    return false;
                }
                ;
            }
            return true;
        }

        public static ConstantRope getConstantRope(int length, String string) {
            StringBuilder sb = new StringBuilder(length);
            sb.insert(0, string);
            sb.setLength(length);
            return new ConstantRope(sb.toString().toCharArray());
        }

        @Specialization(guards = "string == cached_string")
        public static boolean equals(PointerRope date, String string, @Cached("string") String cached_string,
                                     @Cached("getConstantRope(date.length(), string)") ConstantRope value) {
            if (date.length() == value.length()) {
                return equalsLoop(date, value, value.length());
            }
            return false;

        }

    }


    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    static abstract class SplitTextNode extends BuildInFunctionNode {

        public static SplitTextNode create() {
            return AbstractRopeBuiltinsFactory.SplitTextNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1));
        }

        public static AbstractRopeBuiltins.GetCharTextNode getNode() {
            return AbstractRopeBuiltins.GetCharTextNode.create();
        }

        public static int[] getArray() {
            return new int[128];
        }

        @Specialization(guards = "string == cached_string")
        @ExplodeLoop(kind = ExplodeLoop.LoopExplosionKind.FULL_UNROLL)
        public static SplittedRope concat(PointerRope rope,
                                          String string,
                                          @Cached("string") String cached_string,
                                          @Cached("rope.length()") int length,
                                          @Cached(value = "getArray()", dimensions = 0) int[] array,
                                          @Cached(value = "getNode()") GetCharTextNode getNode) {

            char split = cached_string.charAt(0);
           // int[] array = new int[64];
            int c = 0;

            for (int i = 0; i < length; i++) {
                char currentChar = (char) getNode.call(rope, i);
                if (CompilerDirectives.injectBranchProbability(CompilerDirectives.UNLIKELY_PROBABILITY, currentChar == split)) {
                    c++;
                    array[c] = i;
                }
            }
            return new SplittedRope((PointerRope) rope, split, array, c);
            //return new LazySplittedRope((PointerRope) rope, split, length);
        }

    }

    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    static abstract class ConcatTextNode extends BuildInFunctionNode {

        public static ConcatTextNode create() {
            return AbstractRopeBuiltinsFactory.ConcatTextNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1));
        }

        public static ConstantRope getConstantRope(String string) {
            return new ConstantRope(string.toCharArray());
        }

        @Specialization(guards = "string == cached_string")
        public static ConcatRope concat(Rope date, String string, @Cached("string") String cached_string,
                                        @Cached("getConstantRope(string)") ConstantRope value) {
            return new ConcatRope(date, value);

        }
        @Specialization()
        public static ConcatRope concat(Rope date, Rope other) {
            return new ConcatRope(date, other);
        }

    }

    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    public static abstract class GetCharTextNode extends BuildInFunctionNode {

        public static GetCharTextNode create() {
            return AbstractRopeBuiltinsFactory.GetCharTextNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1));
        }

        @Specialization
        public static char get(PointerRope rope, int index) {
            return rope.get(index);
        }

        @Specialization
        public static char get(ConstantRope rope, int index) {
            return rope.get(index);
        }

        GetChildNode getChildNode() {
            return GetChildNode.create();
        }

        @Specialization
        public static char get(ReserveRope rope, int index, @Cached("getChildNode()") GetChildNode childNode) {
            PointerRope childRope = (PointerRope) childNode.call(rope);
            return childRope.get(index);
        }


        @Specialization
        public static char get(ConcatRope rope, int index,
                               @Cached("rope.left.length()") int leftLength,
                               @Cached("create()") GetCharTextNode leftDispatch,
                               @Cached("create()") GetCharTextNode rightDispatch) {
            if (index < leftLength) {
                return (char) leftDispatch.call(rope.left, index);
            } else {
                return (char) rightDispatch.call(rope.right, index - leftLength);
            }
        }

        @Specialization
        public static char get(SubstringRope rope, int index, @Cached("getChildNode()") GetChildNode childNode) {
            PointerRope childRope = (PointerRope) childNode.call(rope);
            return childRope.get(rope.start + index);
        }

        @Specialization
        public static char get(UppercaseRope rope, int index, @Cached("getChildNode()") GetChildNode childNode) {
            PointerRope childRope = (PointerRope) childNode.call(rope);
            return (char) (childRope.get(index) & 0x5f);
        }


        @Specialization
        public static char get(LowercaseRope rope, int index, @Cached("getChildNode()") GetChildNode childNode) {
            PointerRope childRope = (PointerRope) childNode.call(rope);
            return (char) (childRope.get(index) ^ 0x20);
        }

        @Specialization
        public static char get(ArrowSourceRope rope, int index) {
            return rope.get(index);
        }

        @Specialization
        public static char get(CSVSourceRope rope, int index) {
            return rope.get(index);
        }

        @Specialization
        public static char get(AbstractRope rope, int index) {
            return rope.get(index);
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    static abstract class ReverseTextNode extends BuildInFunctionNode {

        public static ReverseTextNode create() {
            return AbstractRopeBuiltinsFactory.ReverseTextNodeGen.create(new ReadArgumentNode(0));
        }

        @Specialization
        public static ReserveRope reverse(AbstractRope rope, @Cached("rope.length()") int length) {
            return new ReserveRope(rope, length);
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    static abstract class UppercaseTextNode extends BuildInFunctionNode {

        public static UppercaseTextNode create() {
            return AbstractRopeBuiltinsFactory.UppercaseTextNodeGen.create(new ReadArgumentNode(0));
        }

        @Specialization
        public static UppercaseRope uppercase(AbstractRope rope) {
            return new UppercaseRope(rope);
        }

    }

    @NodeChild(type = ReadArgumentNode.class)
    static abstract class LowercaseTextNode extends BuildInFunctionNode {

        public static LowercaseTextNode create() {
            return AbstractRopeBuiltinsFactory.LowercaseTextNodeGen.create(new ReadArgumentNode(0));
        }

        @Specialization
        public static LowercaseRope uppercase(AbstractRope rope) {
            return new LowercaseRope(rope);
        }

    }

    @NodeChild(type = ReadArgumentNode.class)
    public static abstract class GetChildNode extends BuildInFunctionNode {

        public static GetChildNode create() {
            return AbstractRopeBuiltinsFactory.GetChildNodeGen.create(new ReadArgumentNode(0));
        }

        @Specialization
        public static PointerRope getChild(ReserveRope date) {
            return (PointerRope) date.child;
        }

        @Specialization
        public static PointerRope getChild(SubstringRope date) {
            return (PointerRope) date.child;
        }

        @Specialization
        public static PointerRope getChild(SplittedRope date) {
            return (PointerRope) date.getRope();
        }

        @Specialization
        public static PointerRope getChild(LazySplittedRope date) {
            return (PointerRope) date.getRope();
        }

        @Specialization
        public static PointerRope getChild(UppercaseRope date) {
            return (PointerRope) date.rope;
        }

        @Specialization
        public static PointerRope getChild(LowercaseRope date) {
            return (PointerRope) date.rope;
        }
    }

    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    @NodeChild(type = ReadArgumentNode.class)
    static abstract class SubstringNode extends BuildInFunctionNode {

        public static SubstringNode create() {
            return AbstractRopeBuiltinsFactory.SubstringNodeGen.create(new ReadArgumentNode(0), new ReadArgumentNode(1), new ReadArgumentNode(2));
        }

        @Specialization
        public static SubstringRope getChild(AbstractRope date, int start, int end) {
            return new SubstringRope(date, start, end);
        }
    }

    @ExportMessage
    static boolean hasLanguage(AbstractRope text) {
        return false;
    }

    @ExportMessage
    static Class<TruffleLanguage<?>> getLanguage(AbstractRope text) throws UnsupportedMessageException {
        return null;
    }

    @ExportMessage
    static Object toDisplayString(AbstractRope text, boolean allowSideEffects) {
        return null;
    }

    @ExportMessage
    public static boolean isString(AbstractRope receiver) {
        return true;
    }


    @ExportMessage
    public static String asString(AbstractRope receiver) throws UnsupportedMessageException {
        return receiver.toString();
    }

}
