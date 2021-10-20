package de.tub.dima.babelfish;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.debug.DebuggerTags;
import com.oracle.truffle.api.instrumentation.ProvidedTags;
import com.oracle.truffle.api.instrumentation.StandardTags;
import de.tub.dima.babelfish.ir.pqp.PQPGenerator;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import de.tub.dima.babelfish.ir.pqp.nodes.BFQueryRootNode;
import de.tub.dima.babelfish.ir.pqp.objects.ExecutableQuery;
import de.tub.dima.babelfish.ir.pqp.nodes.BFQueryCallTarget;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

/**
 * The entrypoint for query execution in babelfish.
 */
@TruffleLanguage.Registration(id = BabelfishEngine.ID,
        name = BabelfishEngine.NAME,
        defaultMimeType = BabelfishEngine.MIME_TYPE,
        characterMimeTypes = BabelfishEngine.MIME_TYPE + "b",
        byteMimeTypes = BabelfishEngine.MIME_TYPE,
        contextPolicy = TruffleLanguage.ContextPolicy.EXCLUSIVE)
@ProvidedTags({StandardTags.CallTag.class, StandardTags.StatementTag.class, StandardTags.RootTag.class, StandardTags.ExpressionTag.class, DebuggerTags.AlwaysHalt.class})
public class BabelfishEngine extends TruffleLanguage<BabelfishEngine.BabelfishContext> {
    public static final String ID = "luth";
    public static final String NAME = "luth";
    public static final String MIME_TYPE = "application/x-luth";

    public static class BabelfishContext {
        private final BabelfishEngine luthLanguage;
        private final TruffleLanguage.Env env;
        private final PQPGenerator astGenerator;

        public BabelfishContext(BabelfishEngine luthLanguage, TruffleLanguage.Env env) {
            this.luthLanguage = luthLanguage;
            this.env = env;
            astGenerator = new PQPGenerator(this);
        }


        public BabelfishEngine getLuthLanguage() {
            return luthLanguage;
        }

        public Env getEnv() {
            return env;
        }
    }

    @Override
    protected boolean isThreadAccessAllowed(Thread thread, boolean singleThreaded) {
        return true;
    }

    @Override
    protected void disposeThread(BabelfishContext context, Thread thread) {
        super.disposeThread(context, thread);
        System.out.println("Dispose Thread " + thread.getName());
    }

    @Override
    protected BabelfishContext createContext(Env env) {
        return new BabelfishContext(this, env);
    }

    @Override
    protected CallTarget parse(ParsingRequest request) throws Exception {
        byte[] byteArray = request.getSource().getBytes().toByteArray();
        ByteArrayInputStream input = new ByteArrayInputStream(byteArray);
        ObjectInputStream inputStream = new ObjectInputStream(input);
        LogicalQueryPlan planObject = (LogicalQueryPlan) inputStream.readObject();
        BFQueryRootNode query = getContextReference().get().astGenerator.generate(planObject);
        ExecutableQuery pipelineCallTarget = new ExecutableQuery(query);
        BFQueryCallTarget queryCallTarget = new BFQueryCallTarget(getContextReference().get().getLuthLanguage(), pipelineCallTarget);
        return Truffle.getRuntime().createCallTarget(queryCallTarget);
    }


    @Override
    protected boolean isObjectOfLanguage(Object object) {
        return false;
    }
}
