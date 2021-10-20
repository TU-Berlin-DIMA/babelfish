package de.tub.dima.babelfish.ir.pqp.objects.state;

public class BFStateManager {

    private static final int MAX_STATE_VARIABLES = 32;

    private final BFStateVariable[] stateVariables = new BFStateVariable[MAX_STATE_VARIABLES];

    public BFStateVariable getStateVariable(int index){
        return stateVariables[index];
    };

    public void setStateVariable(int index, BFStateVariable stateVariable){
        stateVariables[index] = stateVariable;
    }

}
