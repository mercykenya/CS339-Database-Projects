package simpledb.execution;


import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.storage.Tuple;
import java.util.HashMap;
import java.util.Map;

import javax.swing.plaf.basic.BasicBorders.FieldBorder;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import java.util.ArrayList;// Can I import this
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import java.util.NoSuchElementException;




/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;
    private TupleDesc td;
    

    private HashMap<Field, Integer> aggVals;    
    private HashMap<Field, Integer> groupTuples;
    

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // DONE
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.aggVals = new HashMap<>();
        this.groupTuples = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // DONE
        //Get the group by field and aggregate value
        Field gfield = null;
        if (gbfield != Aggregator.NO_GROUPING) {
            gfield = tup.getField(gbfield);
        }
        int aggValue = ((IntField) tup.getField(afield)).getValue();

        // Initialize the original tuple descriptor to ensure that it is the same every time
        if (op == Op.MIN) {
            aggVals.merge(gfield, aggValue, (oldMin, val) -> Math.min(oldMin, val));
        } else if (op == Op.MAX) {
            aggVals.merge(gfield, aggValue, (oldMax, val) -> Math.max(oldMax, val));
        } else if (op == Op.SUM) {
            aggVals.merge(gfield, aggValue, (oldSum, val) -> oldSum + val);
        } else if (op == Op.AVG) {
            aggVals.merge(gfield, aggValue, (oldAvg, val) -> oldAvg + val);
            groupTuples.merge(gfield, 1, (oldCount, val) -> oldCount + 1);
        } else if (op == Op.COUNT) {
            aggVals.merge(gfield, 1, (oldCount, val) -> oldCount + 1);
        }   
        
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // DONE

        // Create a new OpIterator instance
    OpIterator it = new OpIterator() {
        // Define the TupleDesc of the returned tuples
        private final TupleDesc td = (gbfield == Aggregator.NO_GROUPING)
                ? new TupleDesc(new Type[] {Type.INT_TYPE})
                : new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});

        private boolean open;
        private Tuple[] tuples;
        private int currentIdx = 0;

        // Open the iterator and initialize the Tuple array
        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            tuples = new Tuple[aggVals.size()];
            int i = 0;
            if (gbfield == Aggregator.NO_GROUPING) {
                // Compute the aggregate value for the entire table if no grouping
                IntField aggregateVal = (op == Op.AVG)
                        ? new IntField(aggVals.get(null) / groupTuples.get(null))
                        : new IntField(aggVals.get(null));
                tuples[0] = new Tuple(td);
                tuples[0].setField(0, aggregateVal);
            } else {
                // Compute the aggregate value for each group
                for (Map.Entry<Field, Integer> entry : aggVals.entrySet()) {
                    IntField aggregateVal = (op == Op.AVG)
                            ? new IntField(entry.getValue() / groupTuples.get(entry.getKey()))
                            : new IntField(entry.getValue());
                    tuples[i] = new Tuple(td);
                    tuples[i].setField(0, entry.getKey());
                    tuples[i].setField(1, aggregateVal);
                    ++i;
                }
            }
        }

        // Check if there are more tuples to return
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (open) {
                return currentIdx < tuples.length;
            } else {
                throw new IllegalStateException("Iterator not open");
            }
        }

        // Get the next tuple in the sequence
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (open) {
                if (currentIdx < tuples.length) {
                    return tuples[currentIdx++];
                } else {
                    throw new NoSuchElementException("No more tuples");
                }
            } else {
                throw new IllegalStateException("Iterator not open");
            }
        }

        // Reset the iterator to the beginning
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (open) {
                currentIdx = 0;
            } else {
                throw new IllegalStateException("Iterator not open");
            }
        }

        // Get the TupleDesc of the returned tuples
        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        // Close the iterator
        @Override
        public void close() {
            open = false;
            tuples = null;
            currentIdx = 0;
        }
    };

    return it;
    }

}
