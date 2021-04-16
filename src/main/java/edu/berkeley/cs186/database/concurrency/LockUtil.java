package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor locks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        // assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // (proj4_part2): implement
        // If the current lock type can effectively substitute the requested type
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

//        // If the current lock type is IX and the requested lock is S
//        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
//            parentContext.promote(transaction, LockType.SIX);
//            lockContext.acquire(transaction, LockType.S);
//        }
//
//        // If the current lock type is an intent lock
//        if (explicitLockType.isIntent()) {
//            lockContext.promote(transaction, requestType);
//        }

        // require parent locks recursively
        LockType requiredParentLock = LockType.parentLock(requestType);
        if (parentContext != null) {
            ensureSufficientLockHeld(parentContext, requiredParentLock);
            //ensureProperLocksOnAncestors(lockContext, requestType);
        }
        // ensureProperLocksOnAncestors(lockContext, requestType);

        if (lockContext.getExplicitLockType(transaction).equals(LockType.NL)) {
            lockContext.acquire(transaction, requestType);
        } else {
            try {
                lockContext.promote(transaction, requestType);
            } catch (InvalidLockException e) {
                lockContext.escalate(transaction);
                ensureSufficientLockHeld(lockContext, requestType);
            }
        }
    }

    // (proj4_part2) add any helper methods you want
    private static void ensureProperLocksOnAncestors(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction();
        assert transaction != null;

        LockContext parentContext = lockContext.parentContext();
        LockType requiredParentLockType = LockType.parentLock(lockType);

        if (parentContext != null) {
            LockType parentLock = parentContext.getExplicitLockType(transaction);
            if (parentLock.equals(LockType.NL)) {
                ensureProperLocksOnAncestors(parentContext, requiredParentLockType);
                parentContext.acquire(transaction, requiredParentLockType);
            } else if (!LockType.substitutable(parentLock, requiredParentLockType)) {

                while (parentContext != null) {
                    try {
                        parentContext.promote(transaction, requiredParentLockType);
                    } catch (InvalidLockException e) {
                        parentContext.escalate(transaction);
                        ensureSufficientLockHeld(parentContext, requiredParentLockType);
                    }

                    parentContext = parentContext.parentContext();
                    requiredParentLockType = LockType.parentLock(requiredParentLockType);
                }
            }
        }
    }
}
