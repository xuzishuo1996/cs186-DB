package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean compatible(LockType lockType, long except) {
            // (proj4_part1): implement
            for (Lock lock: locks) {
                if (lock.transactionNum != except && !LockType.compatible(lockType, lock.lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Check if there is LockRequest waiting for resource
         */
        public boolean waitingForResource(ResourceName name) {
            for (LockRequest lockRequest : waitingQueue) {
                if (lockRequest.lock.name.equals(name)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // (proj4_part1): implement

            long transactionNum = lock.transactionNum;
            // grant lock
            if (getTransactionLockType(transactionNum).equals(LockType.NL)) {

                transactionLocks.putIfAbsent(transactionNum, new ArrayList<>());
                transactionLocks.get(transactionNum).add(lock);

                locks.add(lock);
            } else {    // update lock

                for (Lock lockForCurrTxn : transactionLocks.get(transactionNum)) {
                    if (lockForCurrTxn.name.equals(lock.name)) {
                        lockForCurrTxn.lockType = lock.lockType;
                    }
                }

                for (Lock lockForResource : locks) {
                    if (lockForResource.transactionNum.equals(lock.transactionNum)) {
                        lockForResource.lockType = lock.lockType;
                    }
                }
            }
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // (proj4_part1): implement

            locks.remove(lock);
            transactionLocks.get(lock.transactionNum).remove(lock);
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // (proj4_part1): implement

            if (addFront) {
                waitingQueue.offerFirst(request);
            } else {
                waitingQueue.offerLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         *
         * The request at the front of the queue is considered, and if it doesn't conflict with any of the existing
         * locks on the resource, it should be removed from the queue and:
         *      1. the transaction that made the request should be given the lock
         *      2. any locks that the request stated should be released are released
         *      3. the transaction that made the request should be unblocked
         * The previous step should be repeated until the first request on the queue cannot be satisfied or the queue is empty.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // (proj4_part1): implement
            while (requests.hasNext()) {
                LockRequest request = requests.next();
                Lock lock = request.lock;
                long transactionNum = request.transaction.getTransNum();

                if (compatible(lock.lockType, transactionNum)) {    // grant the lock

                    grantOrUpdateLock(lock);

                    // the first request in the queue has been processed and remove it
                    requests.remove();

                    // unblock the corresponding transaction
                    request.transaction.unblock();
                } else {
                    break;  // stop
                }
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement

            if (locks.isEmpty()) {
                return LockType.NL;
            }
            List<Lock> locksOfAllResources = transactionLocks.get(transaction);
            if (locksOfAllResources == null || locksOfAllResources.isEmpty()) {
                return LockType.NL;
            }

            ResourceName currResourceName = locks.get(0).name;
            for (Lock lock: locksOfAllResources) {
                if (lock.name.equals(currResourceName)) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Helper method to return the lock held by the transaction on a resource.
     * Returns null if no such lock held.
     */
    private Lock getLock(long transactionNum, ResourceName name) {
        transactionLocks.putIfAbsent(transactionNum, new ArrayList<>());
        List<Lock> locks = transactionLocks.get(transactionNum);
        for (Lock lock: locks) {
            if (lock.name.equals(name)) {
                return lock;
            }
        }
        return null;
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // (proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            // check duplicate lock request
            long transactionNum = transaction.getTransNum();
            if (!getLockType(transaction, name).equals(LockType.NL)) {
                throw new DuplicateLockRequestException("Duplicate lock request from transaction "
                        + transactionNum + " on " + name);
            }

            ResourceEntry resourceEntry = getResourceEntry(name);

            // check if the new lock is not compatible with other locks for the resource
            shouldBlock = !resourceEntry.compatible(lockType, -1);  // no except

            // check if there are other transactions in the queue for the resource
            if (!shouldBlock) {
                shouldBlock = resourceEntry.waitingForResource(name);
            }

            Lock lockToAcquire = new Lock(name, lockType, transactionNum);
            if (!shouldBlock) {
                resourceEntry.grantOrUpdateLock(lockToAcquire);
            } else {
                // create a LockRequest and place it at the end of the waitingQueue
                LockRequest lockRequest = new LockRequest(transaction, lockToAcquire);
                resourceEntry.addToQueue(lockRequest, false);

                transaction.prepareBlock();
            }
        }

        // To block a transaction, call Transaction#prepareBlock inside the synchronized block,
        // and then call Transaction#block outside the synchronized block.
        // The Transaction#prepareBlock needs to be in the synchronized block to avoid a race condition
        // where the transaction may be dequeued between the time it leaves the synchronized block
        // and the time it actually blocks.
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // (proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            long transactionNum = transaction.getTransNum();
            ResourceEntry resourceEntry = getResourceEntry(name);

            // check if a lock on "name" is held by "transaction"
            if (resourceEntry.getTransactionLockType(transactionNum) == LockType.NL) {
                throw new NoLockHeldException("No lock held by " + transactionNum + " for " + name);
            }

            resourceEntry.releaseLock(getLock(transactionNum, name));
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // (proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            long transactionNum = transaction.getTransNum();
            LockType oldLockType = getLockType(transaction, name);
            if (oldLockType.equals(newLockType)) {
                throw new DuplicateLockRequestException("Duplicate lock request from transaction "
                        + transactionNum + " on " + name);
            }
            if (oldLockType.equals(LockType.NL)) {
                throw new NoLockHeldException("No lock held by " + transactionNum + " for " + name);
            }
            // check if it is a valid substitution
            if (!LockType.substitutable(newLockType, oldLockType)) {
                throw new InvalidLockException("Could not promote from " + oldLockType + " to " + newLockType);
            }

            ResourceEntry resourceEntry = getResourceEntry(name);

            // check if the new lock is not compatible with other locks for the resource
            shouldBlock = !resourceEntry.compatible(newLockType, transactionNum);  // no except

            Lock lockToAcquire = new Lock(name, newLockType, transactionNum);
            if (!shouldBlock) {
                resourceEntry.grantOrUpdateLock(lockToAcquire);
            } else {
                // create a LockRequest and place it at the front of the waitingQueue
                LockRequest lockRequest = new LockRequest(transaction, lockToAcquire);
                resourceEntry.addToQueue(lockRequest, true);

                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // (proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        for (Lock lock: resourceEntry.locks) {
            if (lock.transactionNum == transaction.getTransNum()) {
                return lock.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
