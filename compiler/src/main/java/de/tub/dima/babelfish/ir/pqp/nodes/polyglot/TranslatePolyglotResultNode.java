package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.CachedContext;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.object.DynamicObject;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ReadFrameSlotNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.valueTypes.Bool;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_64;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;

import java.util.List;

@NodeChild(value = "polyglotObject", type = BFExecutableNode.class)
public abstract class TranslatePolyglotResultNode extends BFExecutableNode {

    protected final FrameSlot polyglotObjectSlot;
    protected final FrameSlot luthObjectSlot;
    private final BabelfishEngine.BabelfishContext ctx;
    private final FrameDescriptor frameDescriptor;

    public TranslatePolyglotResultNode(BabelfishEngine.BabelfishContext ctx,
                                       FrameDescriptor frameDescriptor,
                                       FrameSlot polyglotObjectSlot,
                                       FrameSlot luthObjectSlot) {
        this.polyglotObjectSlot = polyglotObjectSlot;
        this.luthObjectSlot = luthObjectSlot;
        this.ctx = ctx;
        this.frameDescriptor = frameDescriptor;
    }

    public static TranslatePolyglotResultNode create(BabelfishEngine.BabelfishContext ctx,
                                                     FrameDescriptor frameDescriptor,
                                                     FrameSlot polyglotObjectSlot,
                                                     FrameSlot luthObjectSlot) {
        return TranslatePolyglotResultNodeGen.create(ctx,
                frameDescriptor,
                polyglotObjectSlot,
                luthObjectSlot,
                new ReadFrameSlotNode(polyglotObjectSlot));
    }

    public abstract BFRecord executeAsLuthObject(VirtualFrame frame);

    @Specialization
    public BFRecord translate(BFRecord object) {
        return object;
    }

    @Specialization
    public BFRecord translate(BFType object, @Cached(value = "getSchema()", allowUncached = true) RecordSchema resultSchema) {
        if (CompilerDirectives.inInterpreter()) {
            if (resultSchema.getSize() == 0) {
                resultSchema.addField("value");
            }
        }
        BFRecord resultBFRecord = BFRecord.createObject(resultSchema);
        resultBFRecord.setValue(0, object);
        return resultBFRecord;
    }


    @Specialization
    public BFRecord translate(Integer value,
                              @Cached(value = "getSchema()", allowUncached = true) RecordSchema resultSchema) {
        if (CompilerDirectives.inInterpreter()) {
            if (resultSchema.getSize() == 0) {
                resultSchema.addField("value");
            }
        }
        BFRecord resultBFRecord = BFRecord.createObject(resultSchema);
        resultBFRecord.setValue(0, new Eager_Int_32(value));
        return resultBFRecord;
    }

    @Specialization
    public BFRecord translate(Long value,
                              @Cached(value = "getSchema()", allowUncached = true) RecordSchema resultSchema) {
        if (CompilerDirectives.inInterpreter()) {
            if (resultSchema.getSize() == 0) {
                resultSchema.addField("value");
            }
        }
        BFRecord resultBFRecord = BFRecord.createObject(resultSchema);
        resultBFRecord.setValue(0, new Eager_Int_64(value));
        return resultBFRecord;
    }

    @Specialization
    public BFRecord translate(Double value,
                              @Cached(value = "getSchema()", allowUncached = true) RecordSchema resultSchema) {
        if (CompilerDirectives.inInterpreter()) {
            if (resultSchema.getSize() == 0) {
                resultSchema.addField("value");
            }
        }
        BFRecord resultBFRecord = BFRecord.createObject(resultSchema);
        resultBFRecord.setValue(0, new Eager_Float_64(value));
        return resultBFRecord;
    }

    @Specialization
    public BFRecord translate(Boolean bool,
                              @Cached(value = "getSchema()", allowUncached = true) RecordSchema resultSchema) {
        if (CompilerDirectives.inInterpreter()) {
            if (resultSchema.getSize() == 0) {
                resultSchema.addField("value");
            }
        }
        BFRecord resultBFRecord = BFRecord.createObject(resultSchema);
        resultBFRecord.setValue(0, new Bool(bool));
        return resultBFRecord;
    }

    @Specialization
    public BFRecord translate(String stringValue,
                              @Cached(value = "getSchema()", allowUncached = true) RecordSchema resultSchema) {
        if (CompilerDirectives.inInterpreter()) {
            if (resultSchema.getSize() == 0) {
                resultSchema.addField("value");
            }
        }
        BFRecord resultBFRecord = BFRecord.createObject(resultSchema);
        resultBFRecord.setValue(0, new StringText(stringValue));
        return resultBFRecord;
    }



    @Specialization
    @ExplodeLoop()
    public BFRecord translate(VirtualFrame frame,
                              DynamicObject dynamicPolyglotObject,
                              @CachedLibrary(limit = "30") InteropLibrary interop,
                              @Cached(value = "getSchema()", allowUncached = true) RecordSchema schema,
                              @Cached(value = "createCopyNodes(dynamicPolyglotObject, interop)", allowUncached = true)
                                      BFExecutableNode[] copyNodes) {
        BFRecord resultBFRecord = BFRecord.createObject(schema);
        //System.out.println("IsNumber" + interop.isNumber(dynamicPolyglotObject));
        //TruffleObjectNativeWrapper wraped = TruffleObjectNativeWrapper.wrap(dynamicPolyglotObject);
        //toJavaNode.execute(frame, (PythonNativeObject) dynamicPolyglotObject);
        //Double res = toJavaNode.execute(frame, (PythonNativeObject) dynamicPolyglotObject);

        for (BFExecutableNode node : copyNodes) {
           node.execute(frame);
        }
        return resultBFRecord;
    }


    protected RecordSchema getSchema() {
        RecordSchema r = new RecordSchema();
        r.addField("res");
        return r;
    }

    protected BFExecutableNode[] createCopyNodes(DynamicObject dynamicPolyglotObject,
                                                 InteropLibrary interop) {
        if (interop.hasArrayElements(dynamicPolyglotObject)) {
            try {
                long arraySize = interop.getArraySize(dynamicPolyglotObject);
                return createCopyResultNodesFromArray((int) arraySize);
            } catch (UnsupportedMessageException e) {
                e.printStackTrace();
            }
        }

        return createCopyResultNodes(dynamicPolyglotObject);
    }

    private BFExecutableNode[] createCopyResultNodesFromArray(int arrayCount) {
        BFExecutableNode[] nodes = new BFExecutableNode[arrayCount];
        for (int i = 0; i < arrayCount; i++) {
            nodes[i] = ConvertPolyglotRecordsToBFRecord.create(ctx, frameDescriptor, polyglotObjectSlot, luthObjectSlot, i);
        }
        return nodes;
    }

    private BFExecutableNode[] createCopyResultNodes(DynamicObject evalResultObject) {
        List<Object> keys = evalResultObject.getShape().getKeyList();
        BFExecutableNode[] nodes = new BFExecutableNode[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            nodes[i] = ConvertPolyglotRecordsToBFRecord.create(ctx, frameDescriptor, polyglotObjectSlot, luthObjectSlot, (String) keys.get(i));
        }
        return nodes;
    }


}
