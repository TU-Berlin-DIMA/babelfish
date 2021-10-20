package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.LibraryFactory;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.objects.polyglot.numpy.NumpyWrapper;

/**
 * A scalar polyglot UDF operator for Pythons.
 * This special purpose operator intercepts calls to native python libs like numpy.
 */
public class BFScalarUDFOperatorForPython extends BFScalarUDFOperator {

    @Child
    private InteropLibrary interopLibrary;

    public BFScalarUDFOperatorForPython(TruffleLanguage<?> language,
                                        FrameDescriptor frameDescriptor,
                                        BabelfishEngine.BabelfishContext ctx,
                                        PolyglotUDFOperator udf,
                                        BFOperator next) {
        super(language, frameDescriptor, ctx, udf, next);
        LibraryFactory<InteropLibrary> INTEROP_LIBRARY_ = LibraryFactory.resolve(InteropLibrary.class);
        interopLibrary = INTEROP_LIBRARY_.createDispatched(30);


    }

    @Override
    public void open(VirtualFrame frame) {
        super.open(frame);
         CompilerDirectives.interpreterOnly(() -> {
            try {
                Object udf = interopLibrary.readMember(interopLibrary.readMember(polyglotFunctionObject, "__globals__"), "udf");
                polyglotFunctionObject = (TruffleObject) udf;
                Object globalScope = interopLibrary.readMember(this.polyglotFunctionObject, "__globals__");
                // check if numpy is registered in udf

                //if (interopLibrary.(globalScope, "np")) {
                //Object numpyReference = interopLibrary.readMember(globalScope, "np");
                //if(!(numpyReference instanceof NumpyWrapper)){
                    Object numpyWrapper = new NumpyWrapper(null);
                    interopLibrary.writeMember(globalScope, "np", numpyWrapper);
                //}
                //}


            } catch (UnsupportedMessageException | UnknownIdentifierException | UnsupportedTypeException e) {
                e.printStackTrace();
            }
        });

    }
}
