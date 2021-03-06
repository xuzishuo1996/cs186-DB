package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 *
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            // this.leftSourceIterator = getLeftSource().backtrackingIterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            // mark it to the first entry of the first right page for future reset
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextLeftBlock() {
            // (proj3_part1): implement

            leftBlockIterator = QueryOperator.getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            // mark it to the first entry of the same block for future reset
            leftBlockIterator.markNext();
            if (leftBlockIterator.hasNext()) {
                leftRecord = leftBlockIterator.next();
            }
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextRightPage() {
            // (proj3_part1): implement

            rightPageIterator = QueryOperator.getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);

            // mark it to the first entry of the page for future reset
            rightPageIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         *
         *  Pseudocode:
         *  for each block of B???2 pages B r in R:       --- loop 1
         *      for each page p s in S:                 --- loop 2
         *          for each record r i in B r :        --- loop 3
         *              for each record s j in p s :    --- loop 4
         *                  if ??(r i ,s j ):
         *                      yield <r i , s j >
         */
        private Record fetchNextRecord() {
            // (proj3_part1): implement

            // 4 cases (from inner loop to outer loop), see the pseudocode in above
            while (true) {
                // Case 1 (for loop 4): The right page iterator has a value to yield
                if (rightPageIterator.hasNext()) {
                    nextRecord = rightPageIterator.next();
                    if (compare(leftRecord, nextRecord) == 0) {
                        return leftRecord.concat(nextRecord);   // Record.concat returns a new Record
                    }
                }
                // Case 2 (for loop 3): The right page iterator doesn't have a value to yield but the left block iterator does
                else if (leftBlockIterator.hasNext()) {
                    // [advance left]: within block
                    leftRecord = leftBlockIterator.next();

                    // [reset right]: reset right record iter to the first record of the same page
                    rightPageIterator.reset();
                }
                // Case 3 (for loop 2): Neither the right page nor left block iterators have values to yield,
                // but there's more right pages
                else if (rightSourceIterator.hasNext()) {
                    // [reset left]: reset left record iter to the first record in the curr block of left pages
                    leftBlockIterator.reset();
                    leftRecord = leftBlockIterator.hasNext() ? leftBlockIterator.next() : null;

                    // [advance right]
                    fetchNextRightPage();
                }
                // Case 4 (for loop 1): Neither right page nor left block iterators have values nor are there more right pages,
                // but there are still left blocks
                else if (leftSourceIterator.hasNext()) {
                    // [advance left]: to the next block
                    fetchNextLeftBlock();
                    // NOTE: have already set leftRecord in fetchNextLeftBlock();

                    // [reset right]: reset right record iter to the first record of the FIRST page
                    rightSourceIterator.reset();    // has marked it in the constructor
                    fetchNextRightPage();
                }
                // Upon completion
                else {
                    return null;
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
