package de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.DynamicDispatchLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Wrapper for a pandas data frame. Whenever possible we evaluate functions lazily.
 */
@ExportLibrary(DynamicDispatchLibrary.class)
public class PandasDataframeWrapper implements TruffleObject {

    public final PandasDataframeWrapper parent;

    public final HashMap<String, PandasExpression> modifiedField = new HashMap<>();
    public final Set<String> selectedFields = new HashSet<>();

    @CompilerDirectives.CompilationFinal
    public PandasDataframeWrapper child;

    public PandasDataframeWrapper(PandasDataframeWrapper parent) {
        this.parent = parent;
    }

    public void replaceField(String member, PandasExpression expression){
        modifiedField.put(member, expression);
    }

    public boolean isFieldModified(String member) {
        return modifiedField.containsKey(member);
    }

    public PandasExpression getModifiedField(String member) {
        return modifiedField.get(member);
    }
    public void selectField(String member) {
        selectedFields.add(member);
    }
    public boolean isSelected(String member){
        return selectedFields.isEmpty() || selectedFields.contains(member);
    }



    static class PandasFilteredDataFrame extends PandasDataframeWrapper {
        private final PandasExpression expression;

        PandasFilteredDataFrame(PandasDataframeWrapper parent, PandasExpression expression) {
            super(parent);
            this.expression = expression;
        }

        public PandasExpression getExpression() {
            return expression;
        }

        public BFExecutableNode createPredicateNode(TruffleLanguage lang, FrameDescriptor fr) {
            return expression.createPredicateNode(fr, lang);
        }
    }

    static class PandasModifiedDataFrame extends PandasDataframeWrapper {
        private final PandasExpression expression;

        PandasModifiedDataFrame(PandasDataframeWrapper parent, String member, PandasExpression expression) {
            super(parent);
            this.expression = expression;
        }

        public PandasExpression getExpression() {
            return expression;
        }

    }

    static class PandasRootDataFrame extends PandasDataframeWrapper {
        private final String relation;

        PandasRootDataFrame(String relation) {
            super(null);
            this.relation = relation;
        }

        public String getRelation() {
            return relation;
        }
    }

    static class CountDataFrame extends PandasDataframeWrapper {
        public int count = 0;

        CountDataFrame(PandasDataframeWrapper parent) {
            super(parent);
        }
    }

    static class ConditionalDataFrameAccess extends PandasDataframeWrapper {

        ConditionalDataFrameAccess(PandasDataframeWrapper parent) {
            super(parent);
        }
    }

    public static class ExecuteNextOperator extends PandasDataframeWrapper {
        final BFOperator nextOperator;
        final BFStateManager stateManager;

        public ExecuteNextOperator(PandasDataframeWrapper parent, BFOperator nextOperator, BFStateManager stateManager) {
            super(parent);
            this.nextOperator = nextOperator;
            this.stateManager = stateManager;
        }
    }

    @ExportMessage
    public Class<?> dispatch() {
        return PandasDataframeBuildins.class;
    }
}
