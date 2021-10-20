package de.tub.dima.babelfish.ir.pqp.nodes.state;


import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.storage.UnsafeUtils;

/**
 * Transaction nodes capture the critical section of a state update in NES.
 * function(inputRecord, ctx){
 * <p>
 * state = ctx.getState("stateVariable")
 * ---- start transaction ----
 * state.count++;
 * ---- end transaction ----
 * }
 */
@NodeInfo(shortName = "TransactionNode")
public abstract class BFTransactionNode extends Node {

    private static final int LOCKED_STATE = 1;
    private static final int UNLOCKED_STATE = 0;

    static boolean isLocked(long address) {
        return UnsafeUtils.getIntVolatile(address) == LOCKED_STATE;
    }

    static void spin() {

    }

    ;

    static boolean lock(long address) {
        return UnsafeUtils.UNSAFE.compareAndSwapInt(null, address, UNLOCKED_STATE, LOCKED_STATE);
    }

    ;

    private static void unlock(long address) {
        UnsafeUtils.UNSAFE.compareAndSwapInt(null, address, LOCKED_STATE, UNLOCKED_STATE);
    }

    ;

    @NodeInfo(shortName = "StartTransactionNode")
    public static class StartTransactionNode extends BFTransactionNode {
        /**
         * Starts a new transaction
         *
         * @param address
         */
        public static void getLock(long address) {
            // System.out.println(Thread.currentThread().getName() + ": TryLock" + address);
            do {
                // while (isLocked(address)) {
                spin();
                //}
            } while (!lock(address));
        }

        //@CompilerDirectives.TruffleBoundary(allowInlining = true)
        public void start(long address) {
            getLock(address);
        }
    }

    @NodeInfo(shortName = "EndTransactionNode")
    public static class EndTransactionNode extends BFTransactionNode {
        /**
         * Ends a new transaction
         *
         * @param address
         */
        public void end(long address) {
            unlock(address);
            // System.out.println(Thread.currentThread().getName() + ": Unlock" + address);
        }

    }


}
