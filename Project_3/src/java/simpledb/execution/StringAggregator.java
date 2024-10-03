package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import java.util.Map;
import simpledb.storage.TupleDesc;
import simpledb.storage.Field;
import java.util.HashMap;
import java.util.ArrayList;
import simpledb.storage.IntField;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private Map<Field, Integer> aggVals;
    private Map<Field, Integer> groupTuples;
    private TupleDesc td;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // DONE
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only COUNT operation is supported");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.aggVals = new HashMap<>();
        this.groupTuples = new HashMap<>();
        if (gbfield == Aggregator.NO_GROUPING) {
            this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
        } else {
            this.td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // DONE

        if (aggVals == null) {
            aggVals = new HashMap<Field, Integer>();
            groupTuples = new HashMap<Field, Integer>();
        }
    
        // Extract the group-by field value from the tuple, if there is one
        Field groupByField = null;
        if (gbfield != NO_GROUPING) {
            groupByField = tup.getField(gbfield);
        }
    
        // Extract the aggregate field value from the tuple
        Field aggregateField = tup.getField(afield);
    
        // Update the aggregate value for the appropriate group
        if (aggVals.containsKey(groupByField)) {
            aggVals.put(groupByField, aggVals.get(groupByField) + 1);
        } else {
            aggVals.put(groupByField, 1);
        }
    
        // Update the number of tuples in the appropriate group
        if (groupTuples.containsKey(groupByField)) {
            groupTuples.put(groupByField, groupTuples.get(groupByField) + 1);
        } else {
            groupTuples.put(groupByField, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // DONE
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfield == NO_GROUPING) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(aggVals.get(null)));
            tuples.add(tuple);
        } else {
            for (Field groupVal : groupTuples.keySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, groupVal);
                tuple.setField(1, new IntField(aggVals.get(groupVal)));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
