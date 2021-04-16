package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 *
 * There is exactly ONE LockContext for each resource:
 * calling childContext with the same parameters multiple times returns the same object.
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager. singleton
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Helper method to check whether the requested lock is compliant with its parent locks
     */
    private boolean isValidAcquireRequest(TransactionContext transaction, LockType lockType) {
        if (parent == null) {
            return true;
        }

        List<Lock> parentLocks = lockman.getLocks(parent.name);

        for (Lock lock: parentLocks) {
            if (lock.transactionNum.equals(transaction.getTransNum())
                    && LockType.canBeParentLock(lock.lockType, lockType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // (proj4_part2): implement

        long transactionNum = transaction.getTransNum();

        // check
        if (readonly) {
            throw new UnsupportedOperationException("This LockContext is read-only:\n" + toString());
        }
        if (lockman.getLockType(transaction, name).equals(lockType)) {
            throw new DuplicateLockRequestException("Duplicate lock request from transaction "
                    + transactionNum + " on " + name);
        }
        if (!isValidAcquireRequest(transaction, lockType)) {
            throw new InvalidLockException(
                    String.format("The acquire request for %s on %s from transaction %s is invalid!",
                            lockType, name, transactionNum));
        }

        // invoke underlying LockManager's acquire()
        lockman.acquire(transaction, name, lockType);

        // update numChildLocks
        if (parent != null) {
            Map<Long, Integer> parentNumChildLocks = parent.numChildLocks;
            parentNumChildLocks.putIfAbsent(transactionNum, 0);
            parentNumChildLocks.put(transactionNum, parentNumChildLocks.get(transactionNum) + 1);
        }
    }

    /**
     * Helper to check whether it is valid to release the lock in the hierarchy.
     *
     * It is invalid for the transaction to release the lock on curr resource
     * while holding the locks on child resources.
     */
    private boolean isValidReleaseRequest(TransactionContext transaction) {
        return !(numChildLocks.containsKey(transaction.getTransNum())
                && numChildLocks.get(transaction.getTransNum()) > 0);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // (proj4_part2): implement

        long transactionNum = transaction.getTransNum();

        // check
        if (readonly) {
            throw new UnsupportedOperationException("This LockContext is read-only:\n" + toString());
        }
        if (lockman.getLockType(transaction, name).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held by transaction "
                    + transactionNum + " on " + name);
        }
        if (!isValidReleaseRequest(transaction)) {
            throw new InvalidLockException(
                    String.format("The release request on %s from transaction %s is invalid!",
                            name, transactionNum));
        }

        // invoke underlying LockManager's release()
        lockman.release(transaction, name);

        // update numChildLocks
        if (parent != null) {
            Map<Long, Integer> parentNumChildLocks = parent.numChildLocks;
            parentNumChildLocks.put(transactionNum, parentNumChildLocks.get(transactionNum) - 1);
        }
    }

    /**
     * Helper method to check whether it is valid to promote the lock type.
     */
    private boolean isValidPromoteRequest(TransactionContext transaction, LockType newLockType) {

        // disallow promotion to a SIX lock if an ancestor has SIX, because this would be redundant.
        if (hasSIXAncestor(transaction)) {
            return false;
        }

        LockType oldLockType = lockman.getLockType(transaction, name);

        if (newLockType.equals(LockType.SIX)) {
            return oldLockType.equals(LockType.IS) || oldLockType.equals(LockType.IX) || oldLockType.equals(LockType.S);
        } else {
            return LockType.substitutable(newLockType, oldLockType) && !newLockType.equals(oldLockType);
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * This method performs a lock promotion via the underlying LockManager
     * after ensuring that all multigranularity constraints are met.
     * For example, if the transaction has IS(database) and requests a promotion from S(table) to X(table),
     * the appropriate exception must be thrown (see comments above method).
     *
     * In the special case of promotion to SIX (from IS/IX/S),
     * you should simultaneously release all descendant locks of type S/IS,
     * since we disallow having IS/S locks on descendants when a SIX lock is held.
     *
     * You should also disallow promotion to a SIX lock if an ancestor has SIX,
     * because this would be redundant.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // (proj4_part2): implement

        long transactionNum = transaction.getTransNum();
        LockType oldLockType = lockman.getLockType(transaction, name);

        // check
        if (readonly) {
            throw new UnsupportedOperationException("This LockContext is read-only:\n" + toString());
        }
        if (oldLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held by transaction "
                    + transactionNum + " on " + name);
        }
        if (oldLockType.equals(newLockType)) {
            throw new DuplicateLockRequestException("Duplicate lock request from transaction "
                    + transactionNum + " on " + name);
        }
        if (!isValidPromoteRequest(transaction, newLockType)) {
            throw new InvalidLockException(
                    String.format("The promote request for %s on %s from transaction %s is invalid!",
                            newLockType, name, transactionNum));
        }

        // For promotion to SIX from IS/IX, all S and IS locks on descendants must be simultaneously released.
        if (newLockType.equals(LockType.SIX)
                && (oldLockType.equals(LockType.IS) || oldLockType.equals(LockType.IX))) {

            List<ResourceName> releaseNames = sisDescendants(transaction);
            lockman.acquireAndRelease(transaction, name, newLockType, releaseNames);

            // update numChildLocks
            // Q: synchronized(lockman) ?
            for (ResourceName releaseName : releaseNames) {
                LockContext lockContext = LockContext.fromResourceName(lockman, releaseName);
                LockContext parentContext = lockContext.parentContext();
                parentContext.numChildLocks.put(transactionNum, parentContext.numChildLocks.get(transactionNum) - 1);
            }
        } else {
            // invoke underlying LockManager's promote()
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate all locks on descendants (these are the fine locks) into
     * one lock on the context escalate() was called with (the coarse lock).
     *
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * 1. We are only escalating to S or X. Don't allow escalating to intent locks (IS/IX/SIX).
     * 2. If we only had IS/S locks, we should escalate to S, not X.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // (proj4_part2): implement

        long transactionNum = transaction.getTransNum();

        // check
        if (readonly) {
            throw new UnsupportedOperationException("This LockContext is read-only:\n" + toString());
        }
        if (lockman.getLockType(transaction, name).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held by transaction "
                    + transactionNum + " on " + name);
        }

        // go through child locks
        LockType escalateLockType = LockType.S;
        List<Lock> locks = lockman.getLocks(transaction);
        List<ResourceName> releaseNames = new ArrayList<>();
        for (Lock lock : locks) {
            if (lock.name.equals(name)) {
                releaseNames.add(name);

                if (lock.lockType.equals(LockType.IX) || lock.lockType.equals(LockType.SIX)) {
                    escalateLockType = LockType.X;
                } else if ((lock.lockType.equals(LockType.S) || lock.lockType.equals(LockType.X))
                        /*&& numChildLocks.get(transactionNum) == 0*/) {
                    // prevent repetitive escalate
                    return;
                }
            } else if (lock.name.isDescendantOf(name)) {
                if (lock.lockType.equals(LockType.X) || lock.lockType.equals(LockType.IX)
                        || lock.lockType.equals(LockType.SIX)) {
                    escalateLockType = LockType.X;
                }
                releaseNames.add(lock.name);
            }
        }

        lockman.acquireAndRelease(transaction, name, escalateLockType, releaseNames);

        // update numChildLocks
        // Q: synchronized(lockman) ?
        for (ResourceName releaseName : releaseNames) {
            LockContext lockContext = LockContext.fromResourceName(lockman, releaseName);
            lockContext.numChildLocks.put(transactionNum, 0);
        }
        numChildLocks.put(transactionNum, 0);
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // (proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     *
     * An intent lock does not implicitly grant lock-acquiring privileges to lower levels,
     * if a transaction only has SIX(database), tableContext.getEffectiveLockType(transaction) should return S (not SIX),
     * since the transaction implicitly has S on table via the SIX lock,
     * but not the IX part of the SIX lock (which is only available at the database level).
     * It is possible for the explicit lock type to be one type, and the effective lock type to be a different lock type,
     * specifically if an ancestor has a SIX lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // (proj4_part2): implement

        LockType currType = getExplicitLockType(transaction);
        if (!currType.equals(LockType.NL)) {
            return currType;
        }

        LockContext currContext = parent;
        while (currContext != null && currType.equals(LockType.NL)) {
            currType = currContext.getEffectiveLockType(transaction);
            currContext = currContext.parent;
        }

        if (currType.equals(LockType.IS) || currType.equals(LockType.IX)) {
            return LockType.NL;
        } else if (currType.equals(LockType.SIX)) {
            return LockType.S;
        } else {
            return currType;
        }
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // (proj4_part2): implement

        LockContext currParent = parent;
        while (currParent != null) {
            if (parent.getExplicitLockType(transaction).equals(LockType.SIX)) {
                return true;
            }
            currParent = currParent.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // (proj4_part2): implement

        List<ResourceName> res = new ArrayList<>();

        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name)
                    && (lock.lockType.equals(LockType.S) || lock.lockType.equals(LockType.IS))) {
                res.add(lock.name);
            }
        }
        return res;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

