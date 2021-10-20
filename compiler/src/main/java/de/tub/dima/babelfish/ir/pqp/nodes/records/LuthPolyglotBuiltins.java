package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.object.DynamicObject;
import com.oracle.truffle.api.profiles.ConditionProfile;
import com.oracle.truffle.api.source.Source;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.objects.BFUDFContext;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_64;
import org.graalvm.collections.Pair;

public class LuthPolyglotBuiltins {
    public static final LuthPolyglotBuiltins BUILTINS = new LuthPolyglotBuiltins();


    public static BFExecutableNode createEvalNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode[] args) {
        return LuthPolyglotBuiltinsFactory.PolyglotEvalNodeLuthNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createExecNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode[] args) {
        return LuthPolyglotBuiltinsFactory.PolyglotExecuteNodeLuthNodeGen.create(ctx, args);
    }

   public static BFExecutableNode createExecDirectNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode[] args) {
        return LuthPolyglotBuiltinsFactory.PolyglotExecuteDirectNodeLuthNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createGetNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode... args) {
        return LuthPolyglotBuiltinsFactory.PolyglotGetNodeLuthNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createSetNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode[] args) {
        return LuthPolyglotBuiltinsFactory.SetNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createGetMembersNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode... args) {
        return LuthPolyglotBuiltinsFactory.PolyglotGetMembersNodeLuthNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createGetArraySizeNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode... args) {
        return LuthPolyglotBuiltinsFactory.GetArraySizeNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createHasArrayElementNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode... args) {
        return LuthPolyglotBuiltinsFactory.HasArrayElementsNodeGen.create(ctx, args);
    }


    public static BFExecutableNode createReadArrayElementNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode... args) {
        return LuthPolyglotBuiltinsFactory.ReadArrayElementNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createIsNullNode(BabelfishEngine.BabelfishContext ctx, BFExecutableNode... args) {
        return LuthPolyglotBuiltinsFactory.IsNullNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createIsExecutable(BabelfishEngine.BabelfishContext ctx, BFExecutableNode... args) {
        return LuthPolyglotBuiltinsFactory.IsExecutableNodeGen.create(ctx, args);
    }

    public static BFExecutableNode createAsBoolean(BabelfishEngine.BabelfishContext ctx, BFExecutableNode... args) {
        return LuthPolyglotBuiltinsFactory.AsBooleanNodeGen.create(ctx, args);
    }


    abstract static class SetNode extends LuthBuiltinNodeLuth {

        SetNode(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        protected int getFieldIndex(BFRecord object, String name) {
            if (!object.getObjectSchema().containsField(name)) {
                object.getObjectSchema().addField(name);
            }
            return object.getObjectSchema().getFieldIndexFromConstant(name);
        }

        ;

        @Specialization
        protected Object execute(BFRecord obj, float value, String name, @Cached("getFieldIndex(obj,name)") int fieldIndex) {
            obj.setValue(fieldIndex, new Eager_Float_32(value));
            return null;
        }

        @Specialization
        protected Object execute(BFRecord obj, double value, String name, @Cached("getFieldIndex(obj,name)") int fieldIndex) {
            obj.setValue(fieldIndex, new Eager_Float_64(value));
            return null;
        }

        @Specialization
        protected Object execute(BFRecord obj, int value, String name, @Cached("getFieldIndex(obj,name)") int fieldIndex) {
            obj.setValue(fieldIndex, new Eager_Int_32(value));
            return null;
        }

        @Specialization
        protected Object execute(BFRecord obj, char value, String name, @Cached("getFieldIndex(obj,name)") int fieldIndex) {
            obj.setValue(fieldIndex, new Char(value));
            return null;
        }

        @Specialization
        protected Object execute(BFRecord obj, String value, String name, @Cached("getFieldIndex(obj,name)") int fieldIndex) {
            obj.setValue(fieldIndex, new Char(value.charAt(0)));
            return null;
        }

        @Specialization
        protected Object execute(BFRecord obj, BFType value, String name, @Cached("getFieldIndex(obj,name)") int fieldIndex) {
            obj.setValue(fieldIndex, value);
            return null;
        }
    }


    abstract static class PolyglotGetMembersNodeLuth extends LuthBuiltinNodeLuth {

        PolyglotGetMembersNodeLuth(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @Specialization
        protected Object execute(TruffleObject obj,
                                 @CachedLibrary(limit = "30") InteropLibrary interop) {
            try {
                return interop.getMembers(obj);
            } catch (UnsupportedMessageException e) {
                //throw new Exception("");
                System.out.println(e);
            }
            return null;
        }
    }

    abstract static class PolyglotGetNodeLuth extends LuthBuiltinNodeLuth {

        PolyglotGetNodeLuth(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @Specialization
        protected Object execute(TruffleObject obj, String name, @Cached("name") String name_cached,
                                 @CachedLibrary(limit = "30") InteropLibrary interop) {
            try {
                return interop.readMember(obj, name);
            } catch (UnsupportedMessageException | UnknownIdentifierException e) {
                //throw new Exception("");
                //System.out.println("error");
            }
            return null;
        }
    }

    abstract static class PolyglotExecuteNodeLuth extends LuthBuiltinNodeLuth {
        @CompilerDirectives.CompilationFinal
        public boolean returnBoolean;
        @CompilerDirectives.CompilationFinal
        private RecordSchema outputSchema;

        PolyglotExecuteNodeLuth(BabelfishEngine.BabelfishContext context) {
            super(context);
        }


        @Specialization
        protected Object executeObject(DynamicObject obj,
                                       TruffleObject argument,
                                       BFUDFContext context,
                                       @Cached(value = "obj") DynamicObject obj_cached,
                                       @CachedLibrary(limit = "30") InteropLibrary interop
        ) {
            //TruffleLanguage.Env env = getContext().getEnv();
            // TruffleContext childContext = env.newContextBuilder().build();
            // Object langContext = childContext.enter(this);

            try {
                Object object = interop.execute(obj_cached, argument, context);
                //  childContext.leave(this, langContext);
                // childContext.close();
                return object;
            } catch (UnsupportedTypeException | ArityException | UnsupportedMessageException e) {
                System.out.println(e);
            }
            //childContext.leave(this, langContext);
            return null;
        }

        /*
            if (CompilerDirectives.inInterpreter() && outputSchema == null) {
                outputSchema = new RecordSchema();
                for (int i =0; i< inputSchema.last;i++) {
                    outputSchema.addField(inputSchema.fieldNames[i]);
                }
            }
            BFRecord object = BFRecord.createObject(outputSchema);
            for (int i =0; i< inputSchema.last;i++) {
                object.setValue(i, argument.getValue(i));
            }*/
    }

    abstract static class PolyglotExecuteDirectNodeLuth extends LuthBuiltinNodeLuth {
        @CompilerDirectives.CompilationFinal
        public boolean returnBoolean;
        @CompilerDirectives.CompilationFinal
        private RecordSchema outputSchema;

        PolyglotExecuteDirectNodeLuth(BabelfishEngine.BabelfishContext context) {
            super(context);
        }


        @Specialization
        protected Object executeObject(DynamicObject obj,
                                       @Cached(value = "obj") DynamicObject obj_cached,
                                       @CachedLibrary(limit = "30") InteropLibrary interop
        ) {
            try {
                Object object = interop.execute(obj_cached);
                return object;
            } catch (UnsupportedTypeException | ArityException | UnsupportedMessageException e) {
                //System.out.println(e);
            }
            return null;
        }
    }

    abstract static class PolyglotEvalBaseNodeLuth extends LuthBuiltinNodeLuth {

        protected final ConditionProfile isValid = ConditionProfile.createBinaryProfile();

        PolyglotEvalBaseNodeLuth(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        protected Pair<String, String> getLanguageIdAndMimeType(String languageIdOrMimeType) {
            String languageId = languageIdOrMimeType;
            String mimeType = null;
            if (languageIdOrMimeType.indexOf('/') >= 0) {
                String language = Source.findLanguage(languageIdOrMimeType);
                if (language != null) {
                    languageId = language;
                    mimeType = languageIdOrMimeType;
                }
            }
            return Pair.create(languageId, mimeType);
        }
    }

    abstract static class PolyglotEvalNodeLuth extends PolyglotEvalBaseNodeLuth {

        PolyglotEvalNodeLuth(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @SuppressWarnings("unused")
        @Specialization(guards = {"language.equals(cachedLanguage)"}, limit = "1")
        @CompilerDirectives.TruffleBoundary
        protected Object evalCachedLanguage(String language, String source,
                                            @Cached("language") String cachedLanguage,
                                            @Cached("getLanguageIdAndMimeType(language)") Pair<String, String> languagePair) {
            return evalStringIntl(source, languagePair.getLeft(), languagePair.getRight());
        }

        @Specialization(replaces = "evalCachedLanguage")
        @CompilerDirectives.TruffleBoundary
        protected Object evalString(String language, String source) {
            Pair<String, String> pair = getLanguageIdAndMimeType(language);
            return evalStringIntl(source, pair.getLeft(), pair.getRight());
        }

        private Object evalStringIntl(String sourceText, String languageId, String mimeType) {
            CompilerAsserts.neverPartOfCompilation();

            Source source = Source.newBuilder(languageId, sourceText, "test").mimeType(mimeType).build();
            TruffleLanguage.Env env = getContext().getEnv();

            CallTarget callTarget = null;
            try {
                callTarget = env.parsePublic(source);
            } catch (Exception e) {
                System.out.println(sourceText);
                throw e;
            }
            Object result = callTarget.call();
            return result;
            //callNode = this.insert(Truffle.getRuntime().createDirectCallNode(callTarget));

            /*
            CallTarget callTarget;

            try {
                callTarget = getContext().getRealm().getEnv().parsePublic(source);
            } catch (Exception e) {

            }

            return callTarget.call();*/


        }

    }


    abstract static class GetArraySizeNode extends LuthBuiltinNodeLuth {

        GetArraySizeNode(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @Specialization
        protected Object execute(TruffleObject obj, @CachedLibrary(limit = "30") InteropLibrary interop) {
            try {
                return interop.getArraySize(obj);
            } catch (UnsupportedMessageException e) {
                //throw new Exception("");
                System.out.println(e);
            }
            return null;
        }
    }

    abstract static class ReadArrayElementNode extends LuthBuiltinNodeLuth {

        ReadArrayElementNode(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @Specialization
        protected Object execute(TruffleObject obj, long index, @Cached("index") long cachedIndex, @CachedLibrary(limit = "30") InteropLibrary interop) {
            try {
                return interop.readArrayElement(obj, cachedIndex);
            } catch (UnsupportedMessageException | InvalidArrayIndexException e) {
                System.out.println(e);
            }
            return null;
        }

    }

    abstract static class HasArrayElementsNode extends LuthBuiltinNodeLuth {

        HasArrayElementsNode(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @Specialization
        protected Object execute(TruffleObject obj, @CachedLibrary(limit = "30") InteropLibrary interop) {
            return interop.hasArrayElements(obj);
        }
    }

    abstract static class IsNullNode extends LuthBuiltinNodeLuth {

        IsNullNode(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @Specialization
        protected Object execute(TruffleObject obj, @CachedLibrary(limit = "30") InteropLibrary interop) {
            return interop.isNull(obj);
        }

        @Specialization
        protected Object execute(Object obj, @CachedLibrary(limit = "30") InteropLibrary interop) {
            return false;
        }
    }

    abstract static class IsExecutableNode extends LuthBuiltinNodeLuth {

        IsExecutableNode(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @Specialization
        protected Object isExecutable(TruffleObject obj, @CachedLibrary(limit = "30") InteropLibrary interop) {
            return interop.isExecutable(obj);
        }
    }

    abstract static class AsBooleanNode extends LuthBuiltinNodeLuth {

        AsBooleanNode(BabelfishEngine.BabelfishContext context) {
            super(context);
        }

        @Specialization
        protected boolean asBoolean(boolean obj) {
            return obj;
        }

        @Specialization
        protected boolean asBoolean(int obj) {
            return obj == 1;
        }

        @Specialization
        protected boolean asBoolean(TruffleObject obj, @CachedLibrary(limit = "30") InteropLibrary interop) {
            try {
                return interop.asBoolean(obj);
            } catch (UnsupportedMessageException e) {
                System.out.println(e);
            }
            return false;
        }
    }


}
