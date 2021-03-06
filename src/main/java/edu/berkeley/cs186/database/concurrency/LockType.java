package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     *
     * refer to Compatibility Matrix in TestLockType.java
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // (proj4_part1): implement

        switch (a) {
            case NL: return true;
            case IS: return b != X;
            case IX: return b == NL || b == IS || b == IX;
            case S: return b == NL || b == IS || b == S;
            case SIX: return b == NL || b == IS;
            case X: return b == NL;
            default: return false;
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     *
     * refer to Parent Matrix in TestLockType.java
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // (proj4_part1): implement

        switch (parentLockType) {
            case NL:
            case S:
            case X:
                return childLockType == NL;
            case IS: return (childLockType == NL || childLockType == IS || childLockType == S);
            case IX: return true;
            case SIX: return (childLockType == NL || childLockType == IX || childLockType == X);
            default: return false;
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     *
     * This is only the case if a transaction having substitute can do everything that a transaction having required can do.
     * Another way of looking at this is:
     * let a transaction request the required lock. Can there be any problems if we secretly give it the substitute lock instead?
     *
     * refer to Substitutability Matrix in TestLockType.java
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // (proj4_part1): implement

        switch (substitute) {
            case NL: return required == NL;
            case IS: return (required == NL || required == IS);
            case IX: return (required == NL || required == IS || required == IX);
            case S: return (required == NL || required == S);
            case SIX: return required != X;
            case X: return (required == NL || required == S || required == X);
            default: return false;
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

